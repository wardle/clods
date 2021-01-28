(ns com.eldrix.clods.import.ods
  (:require
    [clojure.core.async :as async]
    [clojure.tools.logging.readable :as log]
    [clojure.data.json :as json]
    [clojure.data.xml :as xml]
    [clojure.data.zip.xml :refer [xml-> xml1-> attr= attr text]]
    [clojure.zip :as zip]
    [clj-bom.core :as bom]
    [com.eldrix.trud.core :as trud]
    [clojure.java.io :as io])
  (:import (java.nio.file Path)
           (java.time LocalDate)
           (java.nio.file.attribute FileAttribute)))

(def supported-ods-xml-version "2-0-0")

(defn manifest
  "Returns manifest information from the ODS XML 'manifest' header"
  [in]
  (with-open [rdr (bom/bom-reader in)]
    (let [data (xml/parse rdr :skip-whitespace true)
          v (first (filter #(= :Manifest (:tag %)) (:content data)))
          root (zip/xml-zip v)]
      {:version            (xml1-> root :Manifest :Version (attr :value))
       :publicationType    (xml1-> root :Manifest :PublicationType (attr :value))
       :publicationDate    (xml1-> root :Manifest :PublicationDate (attr :value))
       :contentDescription (xml1-> root :Manifest :ContentDescription (attr :value))
       :recordCount        (Integer/parseInt (xml1-> root :Manifest :RecordCount (attr :value)))
       })))

(defn parse-concept
  [code-system code]
  {:id          (xml1-> code (attr :id))
   :displayName (xml1-> code (attr :displayName))
   :codeSystem  code-system})

(defn parse-code-system
  [codesystem]
  (let [oid (xml1-> codesystem (attr :oid))]
    {:name  (xml1-> codesystem (attr :name))
     :oid   oid
     :codes (xml-> codesystem :concept (partial parse-concept oid))}))

(defn parse-contact [contact]
  {:type  (xml1-> contact (attr :type))
   :value (xml1-> contact (attr :value))})

(defn parse-contacts [contacts]
  (xml-> contacts :Contact parse-contact))

(defn parse-orgid [orgid]
  {:root                   (xml1-> orgid (attr :root))
   :assigningAuthorityName (xml1-> orgid (attr :assigningAuthorityName))
   :extension              (xml1-> orgid (attr :extension))})

(defn parse-location [l]
  {:address1 (xml1-> l :AddrLn1 text)
   :address2 (xml1-> l :AddrLn2 text)
   :town     (xml1-> l :Town text)
   :county   (xml1-> l :County text)
   :postcode (xml1-> l :PostCode text)
   :country  (xml1-> l :Country text)
   :uprn     (xml1-> l :UPRN text)})

(defn parse-role [role]
  {:id        (xml1-> role (attr :id))
   :isPrimary (let [v (xml1-> role (attr :primaryRole))] (if v (json/read-str v) false))
   :active    (= "Active" (xml1-> role :Status (attr :value)))
   :startDate (xml1-> role :Date :Start (attr :value))
   :endDate   (xml1-> role :Date :End (attr :value))})

(defn parse-roles [roles]
  (xml-> roles :Role parse-role))

(defn parse-succ [succ]
  {:date        (xml1-> succ :Date :Start (attr :value))
   :type        (xml1-> succ :Type text)
   :target      (xml1-> succ :Target :OrgId parse-orgid)
   :primaryRole (xml1-> succ :Target :PrimaryRoleId (attr :id))})

(defn parse-rel [rel]
  {
   :id        (xml1-> rel (attr :id))
   :startDate (xml1-> rel :Date :Start (attr :value))
   :endDate   (xml1-> rel :Date :End (attr :value))
   :active    (= "Active" (xml1-> rel :Status (attr :value)))
   :target    (xml1-> rel :Target :OrgId parse-orgid)})

(defn parse-rels [rels]
  (xml-> rels :Rel parse-rel))

(defn parse-org
  [org]
  (let [roles (xml-> org :Organisation :Roles parse-roles)
        succession (->> (xml-> org :Organisation :Succs :Succ parse-succ)
                        (group-by :type))]
    (merge
      {:orgId          (xml1-> org :Organisation :OrgId parse-orgid)
       :orgRecordClass (keyword (xml1-> org :Organisation (attr :orgRecordClass)))
       :isReference    (let [v (xml1-> org :Organisation (attr :refOnly))] (if v (json/read-str v) false))
       :name           (xml1-> org :Organisation :Name text)
       :location       (xml1-> org :Organisation :GeoLoc :Location parse-location)
       :active         (= "Active" (xml1-> org :Organisation :Status (attr :value)))
       :roles          roles
       :contacts       (xml-> org :Organisation :Contacts parse-contacts)
       :primaryRole    (first (filter :isPrimary roles))
       :relationships  (xml-> org :Organisation :Rels parse-rels)}
      (when-let [predecessors (get succession "Predecessor")]
        {:predecessors predecessors})
      (when-let [successors (get succession "Successor")]
        {:successors successors})
      (when-let [op (xml1-> org :Organisation :Date :Type (attr= :value "Operational"))]
        {:operational {:start (xml1-> (zip/up op) :Start (attr :value))
                       :end   (xml1-> (zip/up op) :End (attr :value))}})
      (when-let [op (xml1-> org :Organisation :Date :Type (attr= :value "Legal"))]
        {:legal {:start (xml1-> (zip/up op) :Start (attr :value))
                 :end   (xml1-> (zip/up op) :End (attr :value))}}))))

(defn- read-organisations
  "Blocking; processes organisations from the TRUD ODS XML file `in`, sending an
  (unparsed) XML fragment representing an organisation to the channel `ch` specified."
  [in ch close?]
  (when-not (= supported-ods-xml-version (:version (manifest in)))
    (throw (ex-info "unsupported ODS XML version." {:expected-version supported-ods-xml-version
                                                    :manifest         (manifest in)})))
  (with-open [rdr (bom/bom-reader in)]
    (->> (:content (xml/parse rdr :skip-whitespace true))
         (filter #(= :Organisations (:tag %)))
         (first)
         (:content)
         (filter #(= :Organisation (:tag %)))
         (run! #(async/>!! ch %))))
  (when close? (async/close! ch)))

(def xf-organisation-xml->map
  "Transducer that takes an organisation XML element (from xml/parse) and parses
  it into a clojure map, removing organisations that are only references."
  (comp (map zip/xml-zip)
        (map parse-org)
        (filter #(not (:isReference %)))))

(defn all-code-systems
  "Returns ODS XML codesystem definitions from the 'codesystems' header as a map of name keyed by oid"
  [in]
  (with-open [rdr (bom/bom-reader in)]
    (let [data (xml/parse rdr :skip-whitespace true)
          v (first (filter #(= :CodeSystems (:tag %)) (:content data)))
          root (zip/xml-zip v)]
      (xml-> root :CodeSystems :CodeSystem parse-code-system))))

(defn all-codes
  "Returns all definitions of all code systems from the ODS XML file specified"
  [in]
  (mapcat :codes (all-code-systems in)))

(defn- org-by-code
  "Returns organisations defined by ODS code (e.g. 7A4BV for UHW, Cardiff) as a demonstration of parsing the ODS XML.
  This ia a private demonstration, rather than intended for operational use."
  [in code]
  (let [ch (async/chan)
        out (async/chan)]
    (async/thread (read-organisations in ch true))
    (async/pipeline 8 out (comp xf-organisation-xml->map
                                (filter #(= code (get-in % [:orgId :extension])))
                                (take 1)) ch)
    (async/<!! out)))

(defn stream-organisations
  [in nthreads batch-size]
  (let [ch (async/chan)
        out (async/chan batch-size (partition-all batch-size))]
    (async/thread (read-organisations in ch true))
    (async/pipeline nthreads out xf-organisation-xml->map ch)
    out))

(defn import-organisations
  "Imports batches of organisations calling back function `f` with batches of organisations."
  [in nthreads batch-size f]
  (let [ch (stream-organisations in nthreads batch-size)]
    (loop [batch (async/<!! ch)]
      (when batch
        (f batch)
        (recur (async/<!! ch))))))

(defn download-ods-xml
  "Downloads the latest ODS distribution file directly from UK TRUD.
  We can use the TRUD tooling to automatically download the release (341).
  This file *should* contain two nested zip files:
   - archive.zip
   - fullfile.zip

  Returns a map containing an inputstream for each file"
  ([api-key] (download-ods-xml api-key nil))
  ([api-key ^LocalDate last-update]
   (let [ch (trud/download-releases api-key [{:release-identifier 341 :release-date last-update}])
         release (clojure.core.async/<!! ch)]
     (when release
       (log/info "Successfully downloaded ODS XML distribution files.")
       (let [download-path (:download-path release)
             archive-path (.resolve download-path "archive.zip")
             fullfile-zip (.resolve download-path "fullfile.zip")
             archive (io/input-stream (trud/file-from-zip (.toFile archive-path)))
             fullfile (io/input-stream (trud/file-from-zip (.toFile fullfile-zip)))]
         {:archive archive
          :full-file fullfile-zip})))))

(defn auto-import-organisations
  "Automatically imports organisational data from the NHS ODS API."
  [api-key nthreads batch-size f]
  (when-let [release (download-ods-xml api-key)]
    (import-organisations (:archive release) nthreads batch-size f)
    (import-organisations (:full-file release) nthreads batch-size f)))

(comment
  (require '[clojure.repl :refer :all])

  (def api-key "xx")
  (def ch (trud/download-releases api-key [341]))
  (def release (clojure.core.async/<!! ch))
  release
  (def download-path (:download-path release))
  download-path
  (def archive-path (.resolve download-path "archive.zip"))
  archive-path
  (def archive-inputstream (trud/file-from-zip (.toFile archive-path)))
  archive-inputstream
  (clojure.java.io/input-stream archive-inputstream)

  (def archive-unzipped (java.nio.file.Files/createTempDirectory "trud" (make-array FileAttribute 0)))
  archive-unzipped
  (trud/unzip (.toFile archive-path) archive-unzipped)

  (def fullfile-path (.resolve download-path "fullfile.zip"))
  (def fullfile-unzipped (java.nio.file.Files/createTempDirectory "trud" (make-array FileAttribute 0)))
  (trud/unzip (.toFile archive-path) archive-unzipped)


  ;; The main ODS data is provided in XML format and available for
  ;; download from https://isd.digital.nhs.uk/trud3/user/authenticated/group/0/pack/5/subpack/341/releases
  (def filename "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Full_20200427.xml")
  (def filename "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Archive_20200427.xml")
  (manifest filename)
  (:version (manifest filename))
  (all-code-systems filename)

  (all-codes filename)
  (org-by-code filename "RRF12")
  (org-by-code filename "7A4BV")

  (def ch (stream-organisations filename 2 10))
  (async/<!! ch)
  (def total (atom 0))
  (import-organisations filename 8 1000
                        (fn [batch]
                          (swap! total + (count batch))
                          (println "\rProcessed " @total)))

  ;; these are the individual steps used by metadata and import-organisations
  (def rdr (-> filename
               bom/bom-reader))
  (def data (xml/parse rdr :skip-whitespace true))
  (def manifest (first (filter #(= :Manifest (:tag %)) (:content data))))
  (def publication-date (get-in (first (filter #(= :PublicationDate (:tag %)) (:content manifest))) [:attrs :value]))
  (def primary-roles (first (filter #(= :PrimaryRoleScope (:tag %)) (:content manifest))))
  (def record-count (get-in (first (filter #(= :RecordCount (:tag %)) (:content manifest))) [:attrs :value]))
  (def root (zip/xml-zip manifest))

  ;; get organisations
  (def orgs (first (filter #(= :Organisations (:tag %)) (:content data))))
  ;; get one organisation
  (def org (first (take 5 (filter #(= :Organisation (:tag %)) (:content orgs)))))

  (json/write-str (parse-org (zip/xml-zip org)))

  ;; get code systems
  (def code-systems (first (filter #(= :CodeSystems (:tag %)) (:content data))))
  (def root (zip/xml-zip code-systems))
  (xml-> root :CodeSystems :CodeSystem :concept (attr :id)) ;; list of identifiers
  (xml-> root :CodeSystems :CodeSystem parse-code-system)

  )



