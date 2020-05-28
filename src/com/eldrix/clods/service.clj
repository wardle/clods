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

(defroutes app-routes
           (context "/v1" []
             (context "/oid" []
             (GET "/:root/:id" [root id]
               (if-let [org (:org (first (jdbc/execute! ds ["SELECT data::varchar as org FROM organisations WHERE id = ?"
                                                            (str root "|" id)])))]
                 {:status  200
                  :headers {"Content-Type" "application/json"}
                  :body    (:org (first (jdbc/execute! ds ["SELECT data::varchar as org FROM organisations WHERE id = ?"
                                                           (str root "|" id)])))}
                 {:status 404})))
             (GET "/search" []
               {:status 500
                :body "Not implemented"}))
           (route/not-found "Not Found"))

(def app
  (wrap-defaults app-routes site-defaults))

(defn start []
  (jetty/run-jetty #'app {:port 3000, :join? false})
  (println "server running in port 3000"))

(comment
  (start)
  )