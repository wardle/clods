(ns com.eldrix.clods.import
  (:require
    [clj-bom.core :as bom]
    [clojure.data.xml :as xml]
    [clojure.data.zip.xml :refer [xml-> xml1-> attr= attr text]]
    [clojure.zip :as zip]
    [next.jdbc :as jdbc]
    [next.jdbc.sql :as sql]
    [clojure.data.json :as json]
    [clojure.string :as str]))

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
   :uprn     (xml1-> l :Uprn text)})

(defn parse-org
  [org]
  (merge
    {
     :orgId          (xml1-> org :Organisation :OrgId parse-orgid)
     :orgRecordClass (xml1-> org :Organisation (attr :orgRecordClass))
     :name           (xml1-> org :Organisation :Name text)
     :location       (xml1-> org :Organisation :GeoLoc :Location parse-location)
     :status  (keyword (xml1-> org :Organisation :Status (attr :value)))
     }
    (when-let [op (xml1-> org :Organisation :Date :Type (attr= :value "Operational"))]
      {:operational {:start (xml1-> (zip/up op) :Start (attr :value))
                     :end   (xml1-> (zip/up op) :End (attr :value))
                     }})
    (when-let [op (xml1-> org :Organisation :Date :Type (attr= :value "Legal"))]
      {:legal {:start (xml1-> (zip/up op) :Start (attr :value))
               :end   (xml1-> (zip/up op) :End (attr :value))}})))

(defn process-organisations
  "Processes organisations from the TRUD ODS XML file, calling fn f with a batch of organisations,
  each a tidied-up version of the original XML more suitable for onward manipulation"
  [in f]
  (with-open [rdr (bom/bom-reader in)]
    (->> (:content (xml/parse rdr :skip-whitespace true))
         (filter #(= :Organisations (:tag %)))
         (first)
         (:content)
         (filter #(= :Organisation (:tag %)))
         (map zip/xml-zip)
         (map parse-org)
         (partition-all 100)
         (run! f))))

(defn manifest
  "Returns manifest information from the ODS XML 'manifest' header"
  [in]
  (with-open [rdr (bom/bom-reader in)]
    (let [data (xml/parse rdr :skip-whitespace true)
          v (first (filter #(= :Manifest (:tag %)) (:content data)))
          root (zip/xml-zip v)]
      {:version         (xml1-> root :Manifest :Version (attr :value))
       :publicationType (xml1-> root :Manifest :PublicationType (attr :value))
       :publicationDate (xml1-> root :Manifest :PublicationDate (attr :value))
       :recordCount     (Integer/parseInt (xml1-> root :Manifest :RecordCount (attr :value)))
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

(defn import-code-systems
  [in ds]
  (let [cs (code-systems in)
        v (map #(vector (:oid %) (:name %))  cs)]
    (with-open [con (jdbc/get-connection ds)
                ps (jdbc/prepare con ["insert into codesystems (oid,name) values (?,?)"])]
      (next.jdbc.prepare/execute-batch! ps v))))

(defn import-codes
  [in ds]
  (let [codes (all-codes in)
        v (map #(vector (:id %) (:displayName %) (:codeSystem %)) codes)]
    (with-open [con (jdbc/get-connection ds)
                ps (jdbc/prepare con ["insert into codes (id,display_name,code_system) values (?,?,?)"])]
      (next.jdbc.prepare/execute-batch! ps v))))

(defn import-orgs
  "Import a batch of organisations"
  [ds orgs]
  (let [v (map #(vector (get-in % [:name]) (json/write-str %)) orgs)]
    (with-open [con (jdbc/get-connection ds)
                ps (jdbc/prepare con ["insert into organisations (name,data) values (?,?::jsonb)"])]
      (next.jdbc.prepare/execute-batch! ps v))))

(defn import-organisations
  [in ds]
  (process-organisations in (partial import-orgs ds)))

(defn import-all
  "Imports organisational data from an ODS XML file"
  [in ds]
  (import-code-systems in ds)
  (import-codes in ds)
  (import-organisations in ds))

(defn org-by-code
  "Returns organisations defined by ODS code (e.g. RWMBV for UHW, Cardiff) as a demonstration of parsing the ODS XML.
  This is an unoptimised search through the ODS XML file and is simply a private demonstration, rather than intended
  for operational use"
  [in code]
  (process-organisations in
                         (fn [orgs]
                           (run! prn (filter #(= code (get-in % [:orgId :extension])) orgs)))))

(comment

  (require '[clojure.repl :refer :all])

  ;; The main ODS data is provided in XML format and available for
  ;; download from https://isd.digital.nhs.uk/trud3/user/authenticated/group/0/pack/5/subpack/341/releases
  (def filename "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Full_20200427.xml")
  (def filename "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Archive_20200427.xml")
  (manifest filename)
  (xml1-> (zip/xml-zip (manifest filename)) :Manifest :Version (attr :value))
  (xml1-> (zip/xml-zip (manifest filename)) :Manifest :FileCreationDateTime (attr :value))
  (:version (manifest filename))
  (code-systems filename)

  (def db {:dbtype "postgresql" :dbname "ods"})
  (def ds (jdbc/get-datasource db))
  (import-code-systems filename ds)
  (import-codes filename ds)
  (import-organisations filename ds)

  ;; these take a while as org-by-code uses a sequential scan, albeit with multiple cores
  (org-by-code filename "RWMBV")                            ;; University Hospital Wales
  (org-by-code filename "W93036")                           ;; Castle Gate surgery

  (org-by-code filename "RRF12")                            ;; first one in the file
  (org-by-code filename "5E115")                            ;; a weird one that isn't parsed correctly

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
  (parse-org (zip/xml-zip org))
  (json/write-str (parse-org (zip/xml-zip org)))

  ;; get code systems
  (def code-systems (first (filter #(= :CodeSystems (:tag %)) (:content data))))
  (def root (zip/xml-zip code-systems))
  (xml-> root :CodeSystems :CodeSystem :concept (attr :id)) ;; list of identifiers
  (xml-> root :CodeSystems :CodeSystem parse-code-system)

  (->> (take 5 (filter #(= :Organisation (:tag %)) (:content orgs)))
       (map parse-xml)
       (map #(vector (get-in % [:Organisation :Name]) (json/write-str (:Organisation %))))))



