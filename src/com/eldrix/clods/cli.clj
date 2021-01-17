(ns com.eldrix.clods.cli
  (:gen-class)
  (:require
    [com.eldrix.clods.import.nhsgp :as nhsgp]
    [com.eldrix.clods.import.nhspd :as nhspd]
    [com.eldrix.clods.import.ods :as ods]
    [com.eldrix.clods.store :as store]
    [com.eldrix.clods.migrations :as migrations]
    [com.eldrix.clods.ws :as ws]
    [clojure.tools.logging :as log]
    [cli-matic.core :refer [run-cmd]]
    [next.jdbc :as jdbc]
    [clojure.core.async :as async]))

(defn do-import-postcodes
  [{db :db args :_arguments}]
  (log/info "Importing postcodes to " db)
  (let [ds (jdbc/get-datasource db)]
    (with-open [conn (.getConnection ds)]
      (doseq [f args]
        (println "Importing postcodes from:" f "...")
        (let [ch (async/chan 1 (partition-all 500))]
          (async/thread (nhspd/import-postcodes f ch))
          (let [total (async/<!! (async/reduce (fn [total batch] (store/insert-postcodes conn batch)
                                                 (+ total (count batch)))
                                               0 ch))]
            (println "Imported " total " postcodes from '" f "'.")))))))

(defn import-all-xml
  "Imports organisational data from an ODS XML file."
  [in ds]
  (with-open [conn (.getConnection ds)]
    (let [mft (ods/manifest in)]
      (log/info "Manifest: " mft)
      (if (= (:version mft) ods/supported-ods-xml-version)
        (do
          (store/insert-code-systems conn (ods/all-code-systems in))
          (store/insert-codes conn (ods/all-codes in))
          (ods/import-organisations in 8 100 (partial store/insert-organisations conn)))
        (log/fatal "unsupported ODS XML version. expected" ods/supported-ods-xml-version "got:" (:version mft))))))

(defn do-init-database
  [{db :db}]
  (with-open [conn (.getConnection (jdbc/get-datasource db))]
    (migrations/init {:connection conn})))

(defn do-migrate-database
  [{db :db}]
  (with-open [conn (.getConnection (jdbc/get-datasource db))]
    (migrations/migrate {:connection conn})))

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
    (nhsgp/download-general-practitioners 1000 (partial store/insert-general-practitioners conn))))

(defn do-serve [{:keys [db port]}]
  (let [ds (store/make-pooled-datasource db)
        svc (store/open-cached-store ds)]
    (log/info "starting server" {:db db :port port})
    (ws/start-server svc port)))

(def CONFIGURATION
  {:app         {:command     "clods"
                 :description "Server and tools for UK organisational data services (ODS)"
                 :version     "0.0.2"}

   :global-opts [{:option "db" :as "JDBC URL to use" :env "DB" :type :string :default "jdbc:postgresql://localhost/ods"}]

   :commands    [
                 {:command     "serve"
                  :description "Run a server."
                  :opts        [{:option "port" :as "Port on which to run server" :type :int :env "HTTP_PORT" :default 8000}]
                  :runs        do-serve}
                 {:command     "init-database"
                  :description "Initialise the database."
                  :opts        []
                  :runs        do-init-database}

                 {:command     "migrate-database"
                  :description "Migrate the database."
                  :opts        []
                  :runs        do-migrate-database}

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

  (migrations/init db)
  (def conn (jdbc/get-connection ds))
  (.close conn)
  (ods/manifest f-full)
  (store/insert-code-systems conn (ods/all-code-systems f-full))
  (store/insert-codes conn (ods/all-codes f-full))
  (ods/import-organisations f-full 4 100 (partial store/insert-organisations conn))
  (ods/import-organisations f-archive 4 100 (partial store/insert-organisations conn))
  (nhsgp/download-general-practitioners 100 (partial store/insert-general-practitioners conn))

  (.close conn)

  )


