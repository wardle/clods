(ns com.eldrix.clods.parse
  (:require
    [clj-bom.core :as bom]
    [clojure.data.xml :as xml]
    [clojure.data.zip.xml :as zx :refer [xml-> xml1-> attr= attr text]]
    [clojure.zip :as zip]
    [clojure.data.json :as json]))

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
   :uprn     (xml1-> l :UPRN text)})

(defn parse-role [role]
  {
   :id        (xml1-> role (attr :id))
   :isPrimary (let [v (xml1-> role (attr :primaryRole))] (if v (json/read-str v) false))
   :status    (keyword (xml1-> role :Status (attr :value)))
   :startDate (xml1-> role :Date :Start (attr :value))
   :endDate   (xml1-> role :Date :End (attr :value))
   })

(defn parse-roles [roles]
  (xml-> roles :Role parse-role))

(defn parse-succ [succ]
  {:date        (xml1-> succ :Succ :Date :Start (attr :value))
   :type        (xml1-> succ :Succ :Type text)
   :target      (xml1-> succ :Succ :Target :OrgId parse-orgid)
   :primaryRole (xml1-> succ :Succ :Target :PrimaryRoleId (attr :id))})

(defn parse-succs [succs]
  (let [vals (->> (xml-> succs :Succ parse-succ)
                  (group-by :type))]
    {
     :predecessors (get vals "Predecessor")
     :successors   (get vals "Successor")
     }))

(defn parse-rel [rel]
  {
   :id        (xml1-> rel (attr :id))
   :startDate (xml1-> rel :Date :Start (attr :value))
   :endDate   (xml1-> rel :Date :End (attr :value))
   :target    (xml1-> rel :Target :OrgId parse-orgid)
   })

(defn parse-rels [rels]
  (xml-> rels :Rel parse-rel))

(defn parse-org
  [org]
  (let [roles (xml-> org :Organisation :Roles parse-roles)]
    (merge
      {:orgId          (xml1-> org :Organisation :OrgId parse-orgid)
       :orgRecordClass (keyword (xml1-> org :Organisation (attr :orgRecordClass)))
       :isReference    (let [v (xml1-> org :Organisation (attr :refOnly))] (if v (json/read-str v) false))
       :name           (xml1-> org :Organisation :Name text)
       :location       (xml1-> org :Organisation :GeoLoc :Location parse-location)
       :status         (keyword (xml1-> org :Organisation :Status (attr :value)))
       :successions    (xml1-> org :Organisation :Succs parse-succs)
       :roles          roles
       :primaryRole    (first (filter :isPrimary roles))
       :relationships  (xml1-> org :Organisation :Rels parse-rels)}
      (when-let [op (xml1-> org :Organisation :Date :Type (attr= :value "Operational"))]
        {:operational {:start (xml1-> (zip/up op) :Start (attr :value))
                       :end   (xml1-> (zip/up op) :End (attr :value))}})
      (when-let [op (xml1-> org :Organisation :Date :Type (attr= :value "Legal"))]
        {:legal {:start (xml1-> (zip/up op) :Start (attr :value))
                 :end   (xml1-> (zip/up op) :End (attr :value))}}))))

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
         (partition-all 10000)
         (run! f))))

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
  (:version (manifest filename))
  (code-systems filename)

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



