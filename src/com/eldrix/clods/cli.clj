(ns com.eldrix.clods.cli
  (:gen-class)
  (:require
    [com.eldrix.clods.import.core :as im]
    [com.eldrix.clods.store :as store]
    [com.eldrix.clods.migrations :as migrations]
    [com.eldrix.clods.updatelog :as updatelog]
    [com.eldrix.clods.ws :as ws]
    [clojure.tools.logging :as log]
    [cli-matic.core :refer [run-cmd]]
    [next.jdbc :as jdbc]
    [com.eldrix.trud.zip :as trudz]
    [com.eldrix.clods.import.ods :as ods]))

(defn import-postcodes
  [{db :db args :_arguments}]
  (log/info "Importing postcodes to " db)
  (let [ds (jdbc/get-datasource db)]
    (with-open [conn (.getConnection ds)]
      (doseq [f args]
        (println "Importing postcodes from:" f "...")
        (let [ch (im/stream-postcodes f)
              total (im/do-batch-count ch (partial store/insert-postcodes conn))]
          (println "Imported " total " postcodes from '" f "'."))))))

(defn init-database
  [{db :db}]
  (with-open [conn (.getConnection (jdbc/get-datasource db))]
    (migrations/init {:connection conn})))

(defn migrate-database
  [{db :db}]
  (with-open [conn (.getConnection (jdbc/get-datasource db))]
    (migrations/migrate {:connection conn})))

(defn download-gps [{db :db}]
  (println "Importing to" db)
  (let [ds (jdbc/get-datasource db)
        conn (.getConnection ds)
        ch (im/stream-general-practitioners)
        total (im/do-batch-count ch (partial store/insert-general-practitioners conn))]
    (println "Processed " total " general practitioner records.")))

(defn download-ods-xml [{:keys [db api-key cache-dir]}]
  (let [ds (jdbc/get-datasource db)]
    (with-open [conn (.getConnection ds)]
      (let [installed (com.eldrix.clods.updatelog/fetch-installed conn)]
        (when-let [results (im/download-ods-xml api-key cache-dir (:uk.nhs.trud/item-294 installed))]
          (when (= 0 (count (:xml-files results)))
            (throw (ex-info "no XML files identified in ODS XML release! Has the structure changed?" {:paths (:paths results)})))
          (com.eldrix.clods.updatelog/log-start-update ds "uk.nhs.trud" "item-294" (get-in results [:release :releaseDate]))
          (doseq [path (:xml-files results)]
            (let [f (.toFile path)
                  mft (ods/manifest f)]
              (log/info "Manifest: " mft)
              (if-not (= (:version mft) ods/supported-ods-xml-version)
                (log/fatal "unsupported ODS XML version. expected" ods/supported-ods-xml-version "got:" (:version mft))
                (do
                  (store/insert-code-systems conn (ods/all-code-systems f))
                  (store/insert-codes conn (ods/all-codes f))
                  (let [ch (ods/stream-organisations f 8 100)
                        total (im/do-batch-count ch (partial store/insert-organisations conn))]
                    (log/info "Processed" total "organisations"))))))
          (com.eldrix.clods.updatelog/log-end-update ds "uk.nhs.trud" "item-294" (get-in results [:release :releaseDate]))
          (trudz/delete-paths (:paths results)))))))

(defn serve [{:keys [db port]}]
  (let [ds (store/make-pooled-datasource db)
        svc (store/open-cached-store ds)]
    (log/info "starting server" {:db db :port port})
    (ws/start-server svc port)))

(def CONFIGURATION
  {:app         {:command     "clods"
                 :description "Server and tools for UK organisational data services (ODS)"
                 :version     "0.0.2"}

   :global-opts [{:option "db"
                  :as "JDBC URL to use"
                  :env "DB"
                  :type :string
                  :default "jdbc:postgresql://localhost/ods"}]

   :commands    [
                 {:command     "serve"
                  :description "Run a server."
                  :opts        [{:option "port" :as "Port on which to run server" :type :int :env "HTTP_PORT" :default 8000}]
                  :runs        serve}
                 {:command     "init-database"
                  :description "Initialise the database."
                  :opts        []
                  :runs        init-database}

                 {:command     "migrate-database"
                  :description "Migrate the database."
                  :opts        []
                  :runs        migrate-database}

                 {:command     "download-ods"
                  :description "Downloads and installs ODS XML data"
                  :opts        [{:as     "TRUD api-key"
                                 :option "api-key"
                                 :type   :slurp}
                                {:as      "Cache directory"
                                 :default "/tmp/trud"
                                 :option  "cache-dir"
                                 :type    :string}]
                  :runs        download-ods-xml}

                 {:command     "download-gps"
                  :description "Downloads and imports/updates GP data. Always import ODS XML data before this step"
                  :opts        []
                  :runs        download-gps}

                 {:command     "import-postcodes"
                  :description "Imports an NHSPD file"
                  :opts        []
                  :runs        import-postcodes}]})

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
  
  )


