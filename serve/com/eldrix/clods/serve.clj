(ns com.eldrix.clods.serve
  "Provides a web service for organisational and geographic health and care data."
  (:gen-class)
  (:require [cheshire.core :as json]
            [clojure.tools.logging.readable :as log]
            [io.pedestal.http :as http]
            [io.pedestal.http.content-negotiation :as conneg]
            [io.pedestal.http.route :as route]
            [io.pedestal.interceptor :as intc]
            [ring.util.response :as ring-response]
            [com.eldrix.clods.core :as clods])
  (:import (java.net URLDecoder)))

(defn response [status body & {:as headers}]
  {:status  status
   :body    body
   :headers headers})

(def ok (partial response 200))
(def not-found (partial response 404))

(def supported-types ["text/html" "application/edn" "application/json" "text/plain"])
(def content-neg-intc (conneg/negotiate-content supported-types))

(defn transform-content
  [body content-type]
  (when body
    (case content-type
      "text/html" body
      "text/plain" body
      "application/edn" (pr-str body)
      "application/json" (json/generate-string body))))

(defn accepted-type
  [context]
  (get-in context [:request :accept :field] "application/json"))

(defn coerce-to
  [response content-type]
  (-> response
      (update :body transform-content content-type)
      (assoc-in [:headers "Content-Type"] content-type)))

(def coerce-body
  {:name ::coerce-body
   :leave
         (fn [context]
           (if (get-in context [:response :headers "Content-Type"])
             context
             (update-in context [:response] coerce-to (accepted-type context))))})

(def entity-render
  "Interceptor to render an entity '(:result context)' into the response."
  {:name :entity-render
   :leave
         (fn [context]
           (if-let [item (:result context)]
             (assoc context :response (ok item))
             context))})

(def get-org
  {:name
   ::get-org
   :enter
   (fn [context]
     (let [svc (get-in context [:request ::service])
           org-id (get-in context [:request :path-params :org-id])
           org (when org-id (clods/fetch-org svc nil org-id))]
       (assoc context :result org)))})

;;  params   : Search parameters; a map containing:
;    |- :s             : search for name or address of organisation
;    |- :n             : search for name of organisation
;    |- :address       : search within address
;    |- :fuzzy         : fuzziness factor (0-2)
;    |- :only-active?  : only include active organisations (default, true)
;    |- :roles         : a string or vector of roles
;    |- :from-postcode : postcode
;    |- :from-lat      : WGS84 latitude
;    |- :from-long     ; WGS84 longitude
;    |- :range          :range
;    |- :limit      : limit on number of search results.
(def search-org
  {:name
   ::search-org
   :enter
   (fn [context]
     (let [svc (get-in context [:request ::service])
           params (get-in context [:request :params])
           {:keys [name from-postcode from-lat from-long range]} params
           range-int (when range (Long/parseLong range))
           params' (cond-> params
                           name
                           (assoc :n name)

                           from-postcode
                           (assoc :from-location {:postcode from-postcode})

                           (and from-lat from-long)
                           (assoc :from-location {:lat from-lat :long from-long})

                           range-int
                           (update-in [:from-location] assoc :range range-int))]
       (assoc context :result (clods/search-org svc params'))))})

(def get-postcode
  {:name
   ::get-postcode
   :enter
   (fn [context]
     (let [svc (get-in context [:request ::service])
           pc (get-in context [:request :path-params :postcode])]
       (if-not pc
         context
         (assoc context :result (clods/fetch-postcode svc pc)))))})

(def common-interceptors [coerce-body content-neg-intc entity-render])
(def routes
  (route/expand-routes
    #{["/ods/v1/search" :get (conj common-interceptors search-org)]
      ["/ods/v1/organisation/:org-id" :get (conj common-interceptors get-org)]
      ["/ods/v1/postcode/:postcode" :get (conj common-interceptors get-postcode)]
      }))

(defn inject-svc
  "A simple interceptor to inject clods service 'svc' into the context."
  [svc]
  {:name  ::inject-svc
   :enter (fn [context] (update context :request assoc ::service svc))})

(def service-map
  {::http/routes routes
   ::http/type   :jetty
   ::http/port   8082})

(defn make-service-map [svc port join?]
  (-> service-map
      (assoc ::http/port port)
      (assoc ::http/join? join?)
      (http/default-interceptors)
      (update ::http/interceptors conj (intc/interceptor (inject-svc svc)))))

(defn start-server
  ([svc port] (start-server svc port true))
  ([svc port join?]
   (http/start (http/create-server (make-service-map svc port join?)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; For interactive development
(defonce server (atom nil))

(defn start-dev [svc port]
  (reset! server
          (http/start (http/create-server (make-service-map svc port false)))))

(defn stop-dev []
  (http/stop @server))

(defn restart [svc port]
  (stop-dev)
  (start-dev svc port))

(defn -main [& args]
  (if-not (= 3 (count args))
    (println "Incorrect parameter. Usage: clj -M:serve <ods index path> <nhspd index path> <port>")
    (let [[ods-path nhspd-path port] args
          svc (clods/open-index ods-path nhspd-path)
          port' (Integer/parseInt port)]
      (log/info "starting NHS ODS server on port " port')
      (log/info "ODS index: " ods-path)
      (log/info "NHSPD index: "nhspd-path)
      (start-server svc port'))))

(comment
  (do
    (require '[io.pedestal.test])
    (defn test-request [verb url]
      (io.pedestal.test/response-for (::http/service-fn @server) verb url))
    (def ods (clods/open-index "/var/tmp/ods" "/tmp/nhspd-2021-02")))
  (start-dev ods 8080)
  (restart ods 8080)
  (test-request :get "/ods/v1/organisation/rwmbv")
  (test-request :get "/ods/v1/postcode/cf14 4XW")
  (clods/fetch-org ods nil "W93036")
  (svc/get-general-practitioner st "G3315839")
  )