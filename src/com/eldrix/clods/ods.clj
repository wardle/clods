(ns com.eldrix.clods.ods
  "Parsing of NHS ODS XML data files.

   The ODS XML file is made up of four components:
   1. The manifest.     : access using `manifest`.
   2. The codesystems.  : access using `all-codesystems`.
   3. The codes.        : access using `all-codes`.
   4. The organisations :.access using `stream-organisations`."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging.readable :as log]
            [clojure.data.json :as json]
            [clojure.data.xml :as xml]
            [clojure.data.zip.xml :refer [xml-> xml1-> attr= attr text]]
            [clojure.zip :as zip]
            [clj-bom.core :as bom]))

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
       :recordCount        (Integer/parseInt (xml1-> root :Manifest :RecordCount (attr :value)))})))

(defn- parse-concept
  [code-system code]
  {:id          (xml1-> code (attr :id))
   :displayName (xml1-> code (attr :displayName))
   :codeSystem  code-system})

(defn- parse-code-system
  [codesystem]
  (let [oid (xml1-> codesystem (attr :oid))]
    {:name  (xml1-> codesystem (attr :name))
     :oid   oid
     :codes (xml-> codesystem :concept (partial parse-concept oid))}))

(defn- parse-contact [contact]
  {:type  (xml1-> contact (attr :type))
   :value (xml1-> contact (attr :value))})

(defn- parse-contacts [contacts]
  (xml-> contacts :Contact parse-contact))

(defn- parse-orgid [orgid]
  {:root                   (xml1-> orgid (attr :root))
   :assigningAuthorityName (xml1-> orgid (attr :assigningAuthorityName))
   :extension              (xml1-> orgid (attr :extension))})

(defn- parse-location [l]
  {:address1 (xml1-> l :AddrLn1 text)
   :address2 (xml1-> l :AddrLn2 text)
   :town     (xml1-> l :Town text)
   :county   (xml1-> l :County text)
   :postcode (xml1-> l :PostCode text)
   :country  (xml1-> l :Country text)
   :uprn     (xml1-> l :UPRN text)})

(defn- parse-role [role]
  {:id        (xml1-> role (attr :id))
   :isPrimary (let [v (xml1-> role (attr :primaryRole))] (if v (json/read-str v) false))
   :active    (= "Active" (xml1-> role :Status (attr :value)))
   :startDate (xml1-> role :Date :Start (attr :value))
   :endDate   (xml1-> role :Date :End (attr :value))})

(defn- parse-roles [roles]
  (xml-> roles :Role parse-role))

(defn- parse-succ [succ]
  {:date        (xml1-> succ :Date :Start (attr :value))
   :type        (xml1-> succ :Type text)
   :target      (xml1-> succ :Target :OrgId parse-orgid)
   :primaryRole (xml1-> succ :Target :PrimaryRoleId (attr :id))})

(defn- parse-rel [rel]
  {:id        (xml1-> rel (attr :id))
   :startDate (xml1-> rel :Date :Start (attr :value))
   :endDate   (xml1-> rel :Date :End (attr :value))
   :active    (= "Active" (xml1-> rel :Status (attr :value)))
   :target    (xml1-> rel :Target :OrgId parse-orgid)})

(defn- parse-rels [rels]
  (xml-> rels :Rel parse-rel))

(defn- parse-org
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

(comment
  (require '[clojure.repl :refer :all])

  ;; The main ODS data is provided in XML format and available for
  ;; download from https://isd.digital.nhs.uk/trud3/user/authenticated/group/0/pack/5/subpack/341/releases
  ;; This code assumes distribution downloaded manually:
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



