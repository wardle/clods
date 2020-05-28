(ns com.eldrix.clods.service
  (:require
    [ring.adapter.jetty :as jetty]
    [clojure.data.json :as json]
    [compojure.core :refer :all]
    [compojure.route :as route]
    [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
    [ring.middleware.params :as params]
    [next.jdbc :as jdbc]))

; TODO: add connection pooling
(def db {:dbtype "postgresql" :dbname "ods"})
(def ds (jdbc/get-datasource db))


(def system->oid {
                "https://fhir.nhs.uk/Id/ods-organization-code" "2.16.840.1.113883.2.1.3.2.4.18.48"
                  "https://fhir.nhs.uk/Id/ods-site-code" "2.16.840.1.113883.2.1.3.2.4.18.48"
                })

(defn get-by-oid [root id]
  (if-let [org (:org (first (jdbc/execute! ds ["SELECT data::varchar as org FROM organisations WHERE id = ?"
                                               (str root "|" id)])))]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (:org (jdbc/execute-one! ds ["SELECT data::varchar as org FROM organisations WHERE id = ?"
                                           (str root "|" id)]))}
    {:status 404}))


(defroutes app-routes
           (GET "/v1/oid/:root/:id" [root id] (get-by-oid root id))
           (GET "/v1/resolve/:id" [id system]
             (get-by-oid (get system->oid system) id))
           (GET "/v1/oid/search" []
             {:status 500
              :body   "Not implemented"})
           (route/not-found "Not Found"))

(def app
  (wrap-defaults app-routes site-defaults))

(defn start []
  (jetty/run-jetty #'app {:port 3000, :join? false})
  (println "server running in port 3000"))

(comment
  (start)
  )