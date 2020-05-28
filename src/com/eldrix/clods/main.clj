(ns com.eldrix.clods.main
  (:gen-class)
  (:require [com.eldrix.clods.ods :as ods]
            [com.eldrix.clods.service :as service]
            [com.eldrix.clods.postcode :as postcode]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log]
            [cli-matic.core :refer [run-cmd]]
            [next.jdbc :as jdbc]))


(defn do-import-postcodes [{db :db args :_arguments}]
  (log/info "Importing postcodes to " db)
  (let [ds (jdbc/get-datasource db)]
    (doseq [f args]
      (println "Importing postcodes from:" f "...")
      (postcode/import-postcodes f ds))))

(defn do-import-gps [{db :db}]
  (println "Importing to" db)
  (let [ds (jdbc/get-datasource db)]
    (ods/import-all-general-practitioners ds)))

(defn do-import-ods-xml [{db :db args :_arguments}]
  (log/info "Importing ODS-XML to" db)
  (let [ds (jdbc/get-datasource db)]
    (doseq [f args]
      (println "Importing ODS-XML from:" f "...")
      (ods/import-all-xml f ds))))

(defn do-serve [{:keys [p db]}]
  (log/info "Starting server on port" p " using database " db)
  (log/fatal "Not implemented")
  )


(def CONFIGURATION
  {:app         {:command     "clods"
                 :description "Server and tools for UK organisational data services (ODS)"
                 :version     "0.0.1"}

   :global-opts [{:option "db" :as "JDBC URL to use" :env "DB" :type :string :default "jdbc:postgresql://localhost/ods"}]

   :commands    [{:command     "serve"
                  :description "Runs a server"
                  :opts        [{:option "p" :as "Port to use" :env "PORT" :type :int :default 8095}]
                  :runs        do-serve}

                 {:command     "import-ods-xml"
                  :description "Imports ODS XML data files. Always import 'full' files before 'archive' files"
                  :opts        []
                  :runs        do-import-ods-xml}

                 {:command     "import-gps"
                  :description "Downloads and imports/updates GP data. Always import ODS XML data before this step"
                  :opts        []
                  :runs        do-import-gps}
                 {:command     "import-postcodes"
                  :description "Imports an NHSPD file"
                  :opts        []
                  :runs        do-import-postcodes}
                 ]})

(defn -main [& args]
  (run-cmd args CONFIGURATION))