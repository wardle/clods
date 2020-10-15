(ns com.eldrix.clods.core
  (:gen-class)
  (:require
    [com.eldrix.clods.db :as store]
    [com.eldrix.clods.ods :as ods]
    [com.eldrix.clods.ws :as ws]
    [com.eldrix.clods.postcode :as postcode]
    [clojure.tools.cli :refer [parse-opts]]
    [clojure.tools.logging :as log]
    [cli-matic.core :refer [run-cmd]]
    [next.jdbc :as jdbc]))


(defn do-import-postcodes
  [{db :db args :_arguments}]
  (log/info "Importing postcodes to " db)
  (let [ds (jdbc/get-datasource db)]
    (with-open [conn (.getConnection ds)]
      (doseq [f args]
        (println "Importing postcodes from:" f "...")
        (postcode/import-postcodes f (partial store/insert-postcodes conn))))))

(defn import-all-xml
  "Imports organisational data from an ODS XML file."
  [in ds]
  (with-open [conn (.getConnection ds)]
    (let [mft (ods/manifest in)]
      (log/info "Manifest: " mft)
      (if (= (:version mft) ods/supported-ods-xml-version)
        (do
          (store/insert-code-systems conn (ods/code-systems in))
          (store/insert-codes conn (ods/all-codes in))
          (ods/import-organisations in 8 1000 (partial store/insert-organisations conn)))
        (log/fatal "unsupported ODS XML version. expected" ods/supported-ods-xml-version "got:" (:version mft))))))

(defn do-import-ods-xml
  [{db :db args :_arguments}]
  (let [ds (jdbc/get-datasource db)]
    (doseq [f args]
      (println "Importing ODS XML from:" f "...")
      (import-all-xml f ds))))

(defn do-import-gps [{db :db}]
  (println "Importing to" db)
  (let [ds (jdbc/get-datasource db)
        conn (.getConnection ds)]
    (ods/download-general-practitioners 1000 (partial store/insert-general-practitioners conn))))

(defn do-serve [{:keys [p db]}]
  (log/info "Starting server on port" p " using database " db)
  (ws/start {:port p}))

(def CONFIGURATION
  {:app         {:command     "clods"
                 :description "Server and tools for UK organisational data services (ODS)"
                 :version     "0.0.2"}

   :global-opts [{:option "db" :as "JDBC URL to use" :env "DB" :type :string :default "jdbc:postgresql://localhost/ods"}]

   :commands    [{:command     "serve"
                  :description "Runs a server"
                  :opts        [{:option "p" :as "Port to use" :env "PORT" :type :int :default 8095}]
                  :runs        do-serve}

                 {:command     "import-ods-xml"
                  :description "Imports ODS XML data files."
                  :opts        []
                  :runs        do-import-ods-xml}

                 {:command     "import-gps"
                  :description "Downloads and imports/updates GP data. Always import ODS XML data before this step"
                  :opts        []
                  :runs        do-import-gps}
                 {:command     "import-postcodes"
                  :description "Imports an NHSPD file"
                  :opts        []
                  :runs        do-import-postcodes}]})

(defn -main [& args]
  (run-cmd args CONFIGURATION))


(comment

  (do
    (def db {:dbtype "postgresql" :dbname "ods"})
    (def ds (jdbc/get-datasource db))
    (def postcode-file "/Users/mark/Downloads/NHSPD_FEB_2020_UK_FULL/Data/nhg20feb.csv")
    (def f-full "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Full_20200427.xml")
    (def f-archive "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Archive_20200427.xml"))

  (def conn (jdbc/get-connection ds))
  (.close conn)
  (store/insert-code-systems conn (ods/code-systems f-full))
  (store/insert-codes conn (ods/all-codes f-full))
  (ods/import-organisations f-full 8 1000 (partial store/insert-organisations conn))
  (ods/import-organisations f-archive 8 1000 (partial store/insert-organisations conn))
  (ods/download-general-practitioners 100 (partial store/insert-general-practitioners conn))
  (postcode/import-postcodes postcode-file (partial store/insert-postcodes conn))

  (.close conn)

  )


