(ns com.eldrix.clods.ods
  "Provides functions for importing organisational data services (ODS) data.
   The complete range of organisational data is made up from multiple sources
   in different formats and 'clods' is designed to abstract and hide this complexity
   as much as possible. Sadly, as far as I am aware TRUD does not provide an API
   to download data files or an easy way to support the automatic identification of
   updated files.

  To confuse matters, many of the downloads available are actually derived from a master
  XML file of organisational data.

  Download the XML file from TRUD: https://isd.digital.nhs.uk/trud3/user/guest/group/0/pack/5/subpack/341/releases
  This needs to be supplemented with additional files from, listed as 'No' under 'Available in XML'
  https://digital.nhs.uk/services/organisation-data-service/data-downloads/gp-and-gp-practice-related-data
  - Current GPs (egpcur)
  - Archived GPs (egparc)"
  (:require
    [clojure.core.async :as async]
    [clojure.data.csv :as csv]
    [clojure.data.json :as json]
    [clojure.data.xml :as xml]
    [clojure.data.zip.xml :refer [xml-> xml1-> attr= attr text]]
    [clojure.java.io :as io]
    [clojure.zip :as zip]
    [clj-bom.core :as bom]
    [clj-http.client :as http])
  (:import
    (java.io File InputStreamReader)
    (java.util.zip ZipFile)))

(def supported-ods-xml-version "2-0-0")

(def ^:private files
  "A list of general practitioner ODS files and their download locations for data not in the master XML file."
  {:egpcur {:name        "egpcur"
            :description "General practitioners"
            :url         "https://files.digital.nhs.uk/assets/ods/current/egpcur.zip"}
   :egparc {:name        "egparc"
            :description "Archived GP Practitioners"
            :url         "https://files.digital.nhs.uk/assets/ods/current/egparc.zip"}})

(def n27-field-format
  "The standard ODS 27-field format headings"
  [:organisationCode
   :name
   :nationalGrouping
   :highLevelHealthGeography
   :address1
   :address2
   :address3
   :address4
   :address5
   :postcode
   :openDate
   :closeDate
   :statusCode
   :subtype
   :parent
   :joinParentDate
   :leftParentDate
   :telephone
   :nil
   :nil
   :nil
   :amendedRecord
   :nil
   :currentOrg
   :nil
   :nil
   :nil])

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
  "Blocking; processes organisations from the TRUD ODS XML file `in`, sending an (unparsed) XML fragment
  representing an organisation to the channel `ch` specified."
  [in ch close?]
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

(defn code-systems
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
  (mapcat :codes (code-systems in)))

(defn- org-by-code
  "Returns organisations defined by ODS code (e.g. RWMBV for UHW, Cardiff) as a demonstration of parsing the ODS XML.
  This ia a private demonstration, rather than intended for operational use."
  [in code]
  (let [ch (async/chan)
        out (async/chan)]
    (async/thread (read-organisations in ch true))
    (async/pipeline 8 out (comp xf-organisation-xml->map
                                (filter #(= code (get-in % [:orgId :extension])))
                                (take 1)) ch)
    (async/<!! out)))

(defn import-organisations
  "Imports batches of organisations calling back function `f` with batches of organisations."
  [in nthreads batch-size f]
  (let [ch (async/chan)
        out (async/chan batch-size (partition-all batch-size))]
    (async/thread (read-organisations in ch true))
    (async/pipeline nthreads out xf-organisation-xml->map ch)
    (loop [batch (async/<!! out)]
      (when batch
        (f batch)
        (recur (async/<!! out))))))

(defn- download [url target]
  (let [request (http/get url {:as :stream})
        buffer-size (* 1024 10)]
    (with-open [input (:body request)
                output (io/output-stream target)]
      (let [buffer (make-array Byte/TYPE buffer-size)]
        (loop []
          (let [size (.read input buffer)]
            (when (pos? size)
              (.write output buffer 0 size)
              (recur))))))))

(defn- file-from-zip
  "Reads from the zipfile specified, extracts the file `filename` and passes each line to your function `f`"
  [zipfile filename f]
  (with-open [zipfile (new ZipFile zipfile)]
    (when-let [entry (.getEntry zipfile filename)]
      (let [reader (InputStreamReader. (.getInputStream zipfile entry))]
        (run! f (csv/read-csv reader))))))

(defn- download-ods-file
  "Blocking; downloads the specified ODS filetype `t` (e.g. :egpcur) returning each item on the channel specified."
  ([t ch] (download-ods-file t ch true))
  ([t ch close?]
   (let [filetype (t files)
         temp (File/createTempFile (:name filetype) ".zip")]
     (when-not filetype
       (throw (IllegalArgumentException. (str "unsupported filetype: " t))))
     (download (:url filetype) temp)
     (file-from-zip temp (str (:name filetype) ".csv")
                    (fn [line]
                      (async/>!! ch (zipmap n27-field-format line))))
     (when close? (async/close! ch))
     (.delete temp))))

(defn download-general-practitioners
  "Downloads data on current and archived general practitioners,
  returning data in batches to the fn `f` specified."
  [batch-size f]
  (let [current (async/chan batch-size (partition-all batch-size))
        archive (async/chan batch-size (partition-all batch-size))]
    (async/thread (download-ods-file :egpcur current))
    (async/thread (download-ods-file :egparc archive))
    (let [merged (async/merge [current archive])]
      (loop [batch (async/<!! merged)]
        (when batch
          (f batch)
          (recur (async/<!! merged)))))))

(comment
  (require '[clojure.repl :refer :all])

  ;; The main ODS data is provided in XML format and available for
  ;; download from https://isd.digital.nhs.uk/trud3/user/authenticated/group/0/pack/5/subpack/341/releases
  (def filename "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Full_20200427.xml")
  (def filename "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Archive_20200427.xml")
  (manifest filename)
  (:version (manifest filename))
  (code-systems filename)

  (all-codes filename)
  (org-by-code filename "RRF12")
  (org-by-code filename "7A4BV")
  (def ch (async/chan))
  (def out (async/chan 5 (partition-all 5)))
  (async/thread (read-organisations filename ch true))
  (async/pipeline 8 out xf-organisation-xml->map ch)
  (async/<!! out)

  (def gps (download-general-practitioners))
  (async/<!! gps)

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

  (def org
    (->> (:content data)
         (filter #(= :Organisations (:tag %)))
         (first)
         (:content)
         (filter #(= :Organisation (:tag %)))
         (map zip/xml-zip)
         (map parse-org)
         (filter #(= "8FL59" (get-in % [:orgId :extension])))
         (first)))


  ;; get code systems
  (def code-systems (first (filter #(= :CodeSystems (:tag %)) (:content data))))
  (def root (zip/xml-zip code-systems))
  (xml-> root :CodeSystems :CodeSystem :concept (attr :id)) ;; list of identifiers
  (xml-> root :CodeSystems :CodeSystem parse-code-system)

  (->> (take 5 (filter #(= :Organisation (:tag %)) (:content orgs)))
       (map parse-xml)
       (map #(vector (get-in % [:Organisation :Name]) (json/write-str (:Organisation %)))))
  )



