(ns com.eldrix.clods.main
  (:gen-class)
  (:require [com.eldrix.clods.importer :as importer]
            [com.eldrix.clods.service :as service]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log]
            [cli-matic.core :refer [run-cmd]]
            [io.pedestal.http :as http]
            [next.jdbc :as jdbc]))

;; This is an adapted service map, that can be started and stopped
;; From the REPL you can call http/start and http/stop on this service
(defonce runnable-service (http/create-server service/service))

(defn run-dev-server
  [& args]
  (log/info "\nCreating [DEV] server...")
  (-> service/service ;; start with production configuration
      (merge {:env :dev
              ;; do not block thread that starts web server
              ::http/join? false
              ;; Routes can be a function that resolve routes,
              ;;  we can use this to set the routes to be reloadable
              ::http/routes #(deref #'service/routes)
              ;; all origins are allowed in dev mode
              ::http/allowed-origins {:creds true :allowed-origins (constantly true)}})
      ;; Wire up interceptor chains
      http/default-interceptors
      http/dev-interceptors
      http/create-server
      http/start))


(defn do-import-gps [{db :db}]
  (println "Importing to" db)
  (let [ds (jdbc/get-datasource db)]
    (importer/import-all-general-practitioners ds)))

(defn do-import-ods-xml [{db :db args :_arguments}]
  (log/info "Importing ODS-XML  to" db)
  (let [ds (jdbc/get-datasource db)]
    (doseq [f args]
      (println "Importing ODS-XML " f "...")
      (importer/import-all f ds))))

(defn do-serve [{:keys [p db]}]
  (log/info "Starting server on port" p " using database " db )
  (http/start runnable-service))


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
                 ]})

(defn -main [& args]
  (run-cmd args CONFIGURATION))