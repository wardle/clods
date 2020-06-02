(ns com.eldrix.clods.service
  (:require
    [ring.adapter.jetty :as jetty]
    [clojure.data.json :as json]
    [compojure.core :refer :all]
    [compojure.route :as route]
    [ring.middleware.json :as middleware]
    [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
    [ring.middleware.params :as params]
    [next.jdbc :as jdbc]
    [com.eldrix.clods.postcode :as postcode]))

; TODO: add connection pooling and runtime configuration
(def db {:dbtype "postgresql" :dbname "ods"})
(def ds (jdbc/get-datasource db))

;; these namespace definitions are published and 'well-known'
(def namespace-ods-organisation "https://fhir.nhs.uk/Id/ods-organization-code")
(def namespace-ods-site "https://fhir.nhs.uk/Id/ods-site-code")
;; these namespace definitions are not published and I've made them up
(def namespace-ods-relationship "https://fhir.nhs.uk/Id/ods-relationship")
(def namespace-ods-succession "https://fhir.nhs.uk/Id/ods-succession")

(defn fetch-org
  ([id] (fetch-org "2.16.840.1.113883.2.1.3.2.4.18.48" id))
  ([root id]
   (when-let [org (:org (jdbc/execute-one! ds ["SELECT data::varchar as org FROM organisations WHERE id = ?"
                                               (str root "|" id)]))]
     (json/read-str org :key-fn keyword))))

(defn get-org [root id]
  (if-let [org (fetch-org root id)]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (merge org
                     (cond
                       (= "RC1" (:orgRecordClass org)) {"@type" namespace-ods-organisation}
                       (= "RC2" (:orgRecordClass org)) {"@type" namespace-ods-site}))}
    {:status 404}))

(defn fetch-code [id]
  (jdbc/execute-one! ds ["SELECT id, display_name, code_system FROM codes where id = ?" id]))

(defn get-code [_ id]
  (if-let [result (fetch-code id)]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    {"@type"      namespace-ods-relationship      ;; made up URL
               :id          (:codes/id result)
               :displayName (:codes/display_name result)
               :codeSystem  (:codes/code_system result)}}
    {:status 404}))

(defn get-postcode [pcode]
  (if-let [pc (:pc (jdbc/execute-one! ds ["SELECT data::varchar as pc FROM postcodes where pcd2 = ?"
                                          (postcode/egif pcode)]))]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    pc}
    {:status 404}))


(def oid->getter {
                  "2.16.840.1.113883.2.1.3.2.4.18.48"  get-org
                  "2.16.840.1.113883.2.1.3.2.4.17.508" get-code})

(def system->oid {
                  namespace-ods-organisation "2.16.840.1.113883.2.1.3.2.4.18.48"
                  namespace-ods-site         "2.16.840.1.113883.2.1.3.2.4.18.48"
                  namespace-ods-relationship "2.16.840.1.113883.2.1.3.2.4.17.508"})

(defn get-oid [oid id]
  (if-let [getter (get oid->getter oid)]
    (getter oid id)
    (route/not-found "Not found")))

(defn org-relationships [org]
  "Turn the list of active relationships in an organisation into a list of identifier triples"
  (->> (:relationships org)
       (filter :active)
       (map #(hash-map
               :subject (merge (:orgId org) {:name (:name org)})
               :predicate (let [code (fetch-code (:id %))]
                            {:root      (:codes/code_system code)
                             :extension (:id %)
                             :name      (:codes/display_name code)})
               :object (merge (:target %) {:name (:name (fetch-org (get-in % [:target :extension])))})))))

(defn org-succession [org k]
  "Turn the successors and predecessors into a list of identifier triples"
  (->> (k org)
       (map #(hash-map
               :subject (merge (:orgId org) {:name (:name org)})
               :predicate {:root      namespace-ods-succession
                           :extension (get {"Predecessor" "resultedFrom"
                                            "Successor"   "resultingOrganization"} (:type %))
                           :date (:date %)}
               :object (merge (:target %) {:name (:name (fetch-org (get-in % [:target :extension])))})))))

(defn get-organisation-properties [id]
  (if-let [org (fetch-org id)]
    (concat
      (org-relationships org)
      (org-succession org :predecessors)
      (org-succession org :successors))
    (route/not-found "Not found")))

(defroutes app-routes
           (GET "/v1/oid/:root/:id" [root id]
             (get-oid root id))
           (GET "/v1/resolve/:id" [system id]
             (if-let [oid (get system->oid system)]
               (get-oid oid id)
               (route/not-found "Not found")))
           (GET "/v1/properties/:id" [system id]
             (if (or (= system namespace-ods-organisation) (= system namespace-ods-site))
               (get-organisation-properties id)
               (route/not-found "Not found")))
           (GET "/v1/postcode/:postcode" [postcode] (get-postcode postcode))
           (GET "/v1/search" []
             {:status 500
              :body   "Not implemented"})
           (route/not-found "Not Found"))

(def app
  (-> app-routes
      (middleware/wrap-json-response)
      (wrap-defaults site-defaults)))

(defn start [{:keys [port is-development]}]
  (jetty/run-jetty #'app {:port (or port 3000) :join? (not is-development)})
  (println "server running in port 3000"))

(comment
  (start {:port 3000 :is-development true})
  )