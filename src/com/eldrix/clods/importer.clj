(ns com.eldrix.clods.importer
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
  - Archived GPs (egparc)

  and the NHSPD - the NHS postcode database"
  (:require
    [com.eldrix.clods.parse :as parse]
    [next.jdbc :as jdbc]
    [clj-http.client :as http]
    [clojure.data.json :as json]
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.tools.logging :as log])
  (:import
    (java.io File InputStreamReader)
    (java.util.zip ZipFile)))

(def supported-ods-xml-version "1-0-0")

(def files

  "A list of general practitioner ODS files and their download locations for data not in the master XML file"
  {:egpcur {:name        "egpcur"
            :description "General practitioners"
            :url         "https://files.digital.nhs.uk/assets/ods/current/egpcur.zip"}
   :egparc {:name        "egparc"
            :description "Archived GP Practitioners"
            :url         "https://files.digital.nhs.uk/assets/ods/current/egparc.zip"}})

(def general-practitioner-org-oid
  "We can safely prepend this oid to organisations referenced in the 27 field format
file to generate a globally unique reference"
  "2.16.840.1.113883.2.1.3.2.4.18.48")

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
   :nil
   ])


(defn download [url target]
  "Download from the url to the target, which will be coerced as per clojure.io/output-stream"
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

(defn file-from-zip [zipfile filename f]
  "Reads from the zipfile specified, extracts the file 'filename' and passes each line to your function 'f'"
  (with-open [zipfile (new ZipFile zipfile)]
    (when-let [entry (.getEntry zipfile filename)]
      (let [reader (InputStreamReader. (.getInputStream zipfile entry))]
        (run! f (csv/read-csv reader))))))

(defn download-ods-file [t f]
  "Downloads the specified ODS filetype 't' (e.g. :egpcur) calling func 'f' with a map representing each item"
  (let [filetype (t files)
        temp (File/createTempFile (:name filetype) ".zip")]
    (download (:url filetype) temp)
    (file-from-zip temp (str (:name filetype) ".csv")
                   (fn [line]
                     (f (zipmap n27-field-format line))))
    (.delete temp)))


(defn import-general-practitioners [t ds]
  (log/info "downloading and importing data from" (get-in files [t :name]) "-" (get-in files [t :description]))
  (with-open [con (jdbc/get-connection ds)
              ps (jdbc/prepare con ["insert into general_practitioners (id, name, organisation, data) values (?,?,?,?::jsonb) on conflict (id) do update set name = EXCLUDED.name, organisation = EXCLUDED.organisation, data = EXCLUDED.data"])]
    (download-ods-file
      t (fn [line]
          (try
            (next.jdbc.prepare/set-parameters ps [(:organisationCode line) (:name line) (str general-practitioner-org-oid "|" (:parent line)) (json/write-str line)])
            (jdbc/execute! ps)
            (catch Exception e (when-not (:leftParentDate line) (log/error e "failed to import: " line ))))))))


(defn import-all-general-practitioners
  [ds]
  (import-general-practitioners :egpcur ds)                 ;; current general practitioners
  (import-general-practitioners :egparc ds))                ;; archived general practitioners

(defn import-code-systems
  [in ds]
  (let [cs (parse/code-systems in)
        v (map #(vector (:oid %) (:name %)) cs)]
    (with-open [con (jdbc/get-connection ds)
                ps (jdbc/prepare con ["insert into codesystems (oid,name) values (?,?) on conflict (oid) do update set name = EXCLUDED.name"])]
      (next.jdbc.prepare/execute-batch! ps v))))

(defn import-codes
  [in ds]
  (let [codes (parse/all-codes in)
        v (map #(vector (:id %) (:displayName %) (:codeSystem %)) codes)]
    (with-open [con (jdbc/get-connection ds)
                ps (jdbc/prepare con ["insert into codes (id,display_name,code_system) values (?,?,?) on conflict (id) do update set display_name = EXCLUDED.display_name, code_system = EXCLUDED.code_system"])]
      (next.jdbc.prepare/execute-batch! ps v))))

(defn import-orgs
  "Import a batch of organisations"
  [ds orgs]
  (let [v (map #(vector
                  (str (get-in % [:orgId :root]) "|" (get-in % [:orgId :extension]))
                  (:name %)
                  (json/write-str %)) orgs)]
    (with-open [con (jdbc/get-connection ds)
                ps (jdbc/prepare con ["insert into organisations (id, name,data) values (?,?,?::jsonb) on conflict (id) do update set name = EXCLUDED.name, data = EXCLUDED.data"])]
      (next.jdbc.prepare/execute-batch! ps v))))

(defn import-organisations
  [in ds]
  (parse/process-organisations in (partial import-orgs ds)))

(defn import-all
  "Imports organisational data from an ODS XML file"
  [in ds]
  (let [mft (parse/manifest in)]
    (log/info "Manifest: " mft)
    (if (= (:version mft) supported-ods-xml-version)
      (do (import-code-systems in ds)
          (import-codes in ds)
          (import-organisations in ds))
      (log/fatal "unsupported ODS XML version. expected" supported-ods-xml-version "got:" (:version mft)))))

(comment

  (def db {:dbtype "postgresql" :dbname "ods"})
  (def ds (jdbc/get-datasource db))
  (def f-full "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Full_20200427.xml")
  (def f-archive "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Archive_20200427.xml")

  (import-code-systems filename ds)
  (import-codes filename ds)
  (import-organisations f-full ds)
  (import-organisations f-archive ds)
  (import-general-practitioners :egpcur ds)
  (import-general-practitioners :egparc ds)
  )