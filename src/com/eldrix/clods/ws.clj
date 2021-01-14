(ns com.eldrix.clods.ws
  "Provides a simple REST-like API for health and care organisational and
  geographical data."
  (:require
    [com.eldrix.clods.svc :as svc]
    [com.eldrix.clods.resolve :as res]
    [io.pedestal.http :as http]
    [io.pedestal.http.content-negotiation :as conneg]
    [io.pedestal.http.route :as route]
    [io.pedestal.interceptor :as intc]
    [io.pedestal.interceptor.error :as intc-err]
    [cheshire.core :as json]
    [clojure.tools.logging.readable :as log]
    [ring.util.response :as ring-resp]
    [com.eldrix.clods.store :as store]))


(def uri->oid {"2.16.840.1.113883.2.1.3.2.4.18.48"          "2.16.840.1.113883.2.1.3.2.4.18.48"
               res/namespace-ods-organisation               "2.16.840.1.113883.2.1.3.2.4.18.48"
               "urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48"  "2.16.840.1.113883.2.1.3.2.4.18.48"
               res/namespace-ods-site                       "2.16.840.1.113883.2.1.3.2.4.18.48"
               res/namespace-ods-relationship               "2.16.840.1.113883.2.1.3.2.4.17.508"
               "urn:oid:2.16.840.1.113883.2.1.3.2.4.17.508" "2.16.840.1.113883.2.1.3.2.4.17.508"})

;;(def resolvers {res/namespace-ods-organisation               http-get-org
;;                res/namespace-ods-site                       http-get-org
;;                res/namespace-ods-relationship               http-get-code
;;                res/namespace-os-postcode                    http-get-postcode
;;                "urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48"  http-get-org
;;                "2.16.840.1.113883.2.1.3.2.4.18.48"          http-get-org
;;                "urn:oid:2.16.840.1.113883.2.1.3.2.4.17.508" http-get-code
;;                "2.16.840.1.113883.2.1.3.2.4.17.508"         http-get-code})

;; (defn http-resolve [system value]
;;   (if-let [resolver (get resolvers system)]
;;     (resolver system value)
;;     (route/not-found "Not found")))
;;
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
           org (when org-id (svc/get-org svc org-id))
           norg (res/normalize-org org)]
       (if-not norg
         context
         (assoc context
           :result
           (if (:active norg)
             norg
             (assoc norg :replacedBy (map res/normalize-org (res/active-successors svc org))))))))})

;; TODO: add more input validation / limits
(def search-org
  {:name
   ::search-org
   :enter
   (fn [context]
     (let [svc (get-in context [:request ::service])
           params (get-in context [:request :params])]
       (assoc context :result (svc/search-org svc params))))})

(def get-org-gps
  "Return a list of general practitioners for a given GP practice."
  {:name
   ::get-org-gps
   :enter
   (fn [context]
     (let [svc (get-in context [:request ::service])
           org-id (get-in context [:request :path-params :org-id])]
       (if-not org-id
         context
         (assoc context :result (svc/get-general-practitioners-for-org svc org-id)))))})

(def get-code
  {:name
   ::get-code
   :enter
   (fn [context]
     (let [svc (get-in context [:request ::service])
           code (get-in context [:request :path-params :code])]
       (if-not code
         context
         (assoc context :result (svc/get-code svc code)))))})

(def get-postcode
  {:name
   ::get-postcode
   :enter
   (fn [context]
     (let [svc (get-in context [:request ::service])
           pc (get-in context [:request :path-params :postcode])]
       (if-not pc
         context
         (assoc context :result (svc/get-postcode svc pc)))))})

(def common-interceptors [coerce-body content-neg-intc entity-render])
(def routes
  (route/expand-routes
    #{["/ods/v1/search" :get (conj common-interceptors search-org)]
      ["/ods/v1/organisation/:org-id" :get (conj common-interceptors get-org)]
      ["/ods/v1/organisation/:org-id/general-practitioners" :get (conj common-interceptors get-org-gps)]
      ["/ods/v1/code/:code" :get (conj common-interceptors get-code)]
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


(comment
  (do
    (require '[next.jdbc])
    (require '[next.jdbc.connection :as connection])
    (require '[com.eldrix.clods.store])
    (require '[io.pedestal.test])
    (defn test-request [verb url]
      (io.pedestal.test/response-for (::http/service-fn @server) verb url))
    (def ds (next.jdbc/get-datasource "jdbc:postgresql://localhost/ods"))
    (def conn (connection/->pool com.zaxxer.hikari.HikariDataSource {:jdbcUrl         "jdbc:postgresql://localhost/ods"
                                                                     :maximumPoolSize 10}))
    (def st (store/new-cached-store conn)))
  (start-dev st 8080)
  (restart st 8080)
  (test-request :get "/ods/v1/organisation/rwmbv")
  (test-request :get "/ods/v1/code/RO72")
  (test-request :get "/ods/v1/postcode/cf14 4XW")
  (test-request :get "/ods/v1/organisation/W93036/general-practitioners")
  (def org (svc/get-org st "7A4"))
  (res/normalize-org org)
  (svc/get-org st "W93036")
  (svc/get-general-practitioner st "G3315839")

  namespace->uri
  (def test-kw :org.w3.www.ns.prov/prefLabel)

  (ns->uri :org.w3.www.2004.02.skos.core/prefLabel)
  (def props {:org.w3.www.ns.prov/prefLabel      "UHW"
              :org.w3.www.ns.prov/wasDerivedFrom [:org.nhs.fhir.id.ods-site-code "RWMBV"]})

  (->> props
       (map #(vector (ns->uri (first %)) (second %))))
  )
