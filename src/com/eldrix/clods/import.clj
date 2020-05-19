(ns com.eldrix.clods.import
  (:require
    [clj-bom.core :as bom]
    [clojure.data.xml :as xml]
    [camel-snake-kebab.core :as csk]
    [camel-snake-kebab.extras :as cske]))



;; based on http://rosario.io/2016/12/26/convert-xml-json-clojure
;; and https://github.com/rosario/json-parse/blob/master/src/json_parse/core.clj
;; with tweaks for brain-dead NHS Digital ODS usage of XML
(defn different-keys? [content]
  (when content
    (let [dkeys (count (filter identity (distinct (map :tag content))))
          n (count content)]
      (= dkeys n))))

(defn xml->json [element]
  "Converts XML into a more easily parseable set of clojure structures"
  (cond
    (nil? element) nil
    (string? element) element
    (sequential? element) (if (> (count element) 1)
                            (if (different-keys? element)
                              (reduce into {} (map (partial xml->json) element))
                              (map xml->json element))
                            (xml->json (first element)))
    (and (map? element) (empty? element)) {}
    ;; handle unconventional use of XML attributes with a single value= attribute and no actual value (content)
    (and (map? element) (empty? (:content element)) (= 1 (count (:attrs element))) (contains? (:attrs element) :value))
    {(csk/->kebab-case (:tag element)) (xml->json (get-in element [:attrs :value]))}
    ;; handle an element with attributes and some content - if content a map, merge. Otherwise merge content as :value
    (map? element) (let [v (xml->json (:content element))
                         attrs (cske/transform-keys csk/->kebab-case (:attrs element))
                         tag (csk/->kebab-case (:tag element))]
                     (if (seq attrs) {tag (cond (nil? v) attrs
                                                (map? v) (merge attrs v)
                                                :else (merge attrs {:value v}))}
                                     {tag (xml->json (:content element))}))
    :else nil))

(defn import-organisations
  "Imports organisations from the TRUD ODS XML file, calling fn f with a batch of organisations,
  each a tidied-up version of the original XML a bit more suitable for onward manipulation"
  [in f]
  (with-open [rdr (bom/bom-reader in)]
    (->> (:content (xml/parse rdr :skip-whitespace true))
         (filter #(= :Organisations (:tag %)))
         (first)
         (:content)
         (filter #(= :Organisation (:tag %)))
         (map xml->json)
         (partition-all 100)
         (run! f))))

(defn metadata
  "Returns ODS XML metadata information from the 'manifest' header"
  [in]
  (with-open [rdr (bom/bom-reader in) ]
    (let [data (xml/parse rdr :skip-whitespace true)
          manifest (first (filter #(= :Manifest (:tag %)) (:content data)))]
      (:manifest(xml->json manifest)))))

(defn find-by-code
  "Returns organisations defined by ODS code (e.g. RWMBV for UHW, Cardiff) as a demonstration of parsing the ODS XML.
  This is an unoptimised search through the ODS XML file and is simply a private demonstration, rather than intended
  for operational use"
  [in code]
  (import-organisations in
    (fn [orgs]
      (run! prn (filter #(= code (get-in % [:organisation :org-id :extension])) orgs) ))))

(comment

  (require '[clojure.repl :refer :all])

  ;; The main ODS data is provided in XML format and available for
  ;; download from https://isd.digital.nhs.uk/trud3/user/authenticated/group/0/pack/5/subpack/341/releases
  (def filename "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Full_20200427.xml")
  (def filename "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Archive_20200427.xml")
  (metadata filename)
  (:record-count (metadata filename))
  (:file-creation-date-time (metadata filename))
  ;; these take a while as find-by-code uses a sequential scan, albeit with multiple cores
  (find-by-code filename "RWMBV")                                    ;; University Hospital Wales
  (find-by-code filename "W93036")                                    ;; Castle Gate surgery

  ;; these are the individual steps used by metadata and import-organisations
  (def rdr (-> filename
               bom/bom-reader))
  (def data (xml/parse rdr :skip-whitespace true))
  (def manifest (first (filter #(= :Manifest (:tag %)) (:content data))))
  (def publication-date (get-in (first (filter #(= :PublicationDate (:tag %)) (:content manifest))) [:attrs :value]))
  (def primary-roles (first (filter #(= :PrimaryRoleScope (:tag %)) (:content manifest))))
  (def record-count (get-in (first (filter #(= :RecordCount (:tag %)) (:content manifest))) [:attrs :value]))
  ;; get organisations
  (def orgs (first (filter #(= :Organisations (:tag %)) (:content data))))
  ;; get one organisation
  (def org (first (take 5 (filter #(= :Organisation (:tag %)) (:content orgs)))))
  (xml->json org)
  )