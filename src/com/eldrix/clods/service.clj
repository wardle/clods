(ns com.eldrix.clods.service
  (:require
    [ring.adapter.jetty :as jetty]
    [clojure.data.json :as json]
    [compojure.core :refer :all]
    [compojure.route :as route]
    [ring.middleware.json :as middleware]
    [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
    [ring.middleware.params :as params]
    [com.wsscode.pathom.core :as p]
    [com.wsscode.pathom.connect :as pc]
    [clojure.core.async :refer [<!!]]
    [clojure.string :as str]
    [next.jdbc :as jdbc]
    [com.eldrix.clods.postcode :as postcode]))

; TODO: add connection pooling and runtime configuration
(def db {:dbtype "postgresql" :dbname "ods"})
(def ds (jdbc/get-datasource db))

;; these namespace definitions are published and 'well-known'
(def namespace-ods-organisation "https://fhir.nhs.uk/Id/ods-organization-code")
(def namespace-ods-site "https://fhir.nhs.uk/Id/ods-site-code")
(def namespace-os-postcode "http://data.ordnancesurvey.co.uk/ontology/postcode")

;; these namespace definitions are not published and I've made them up (!) ...
;; TODO: use "standards" when we make them
(def namespace-ods-relationship "https://fhir.nhs.uk/Id/ods-relationship")
(def namespace-ods-succession "https://fhir.nhs.uk/Id/ods-succession")



;; map clojure/java type namespaces (packages) into URIs
(def namespace->uri {
                     "org.w3.www.2001.01.rdf-schema"        "http://www.w3.org/2000/01/rdf-schema#"
                     "org.w3.www.2001.XMLSchema"            "http://www.w3.org/2001/XMLSchema#"
                     "org.w3.www.2002.07.owl"               "http://www.w3.org/2002/07/owl#"
                     "org.w3.www.ns.prov"                   "http://www.w3.org/ns/prov#"
                     "uk.nhs.fhir.id.ods-organization-code" "https://fhir.nhs.uk/Id/ods-organization-code#"
                     "uk.nhs.fhir.id.ods-site-code"         "https://fhir.nhs.uk/Id/ods-site-code"})





(defn fetch-org
  "Fetches an organisation by `root` and `identifier` from the data store"
  ([id] (fetch-org "2.16.840.1.113883.2.1.3.2.4.18.48" id))
  ([root id]
   (when-let [org (:org (jdbc/execute-one! ds
                                           ["SELECT data::varchar as org FROM organisations WHERE id = ?"
                                            (str root "|" id)]))]
     (json/read-str org :key-fn keyword))))

(defn fetch-code [id]
  (jdbc/execute-one! ds
                     ["SELECT id, display_name, code_system FROM codes where id = ?" id]))

(defn fetch-postcode [pcode]
  (when-let [pc (:pc (jdbc/execute-one! ds
                                        ["SELECT data::varchar as pc FROM postcodes where pcd2 = ?"
                                         (postcode/egif pcode)]))]
    (json/read-str pc :key-fn keyword)))

(def orgRecordClass->namespace {"RC1" namespace-ods-organisation
                                "RC2" namespace-ods-site})

(defn normalize-id
  "Normalizes an ODS identifier oid/extension to a URI/value with the URI
  prefix of 'urn:uri:'"
  [id]
  (-> id
      (dissoc :root :extension)
      (assoc :system (str "urn:oid:" (:root id))
             :value (:extension id))))

(defn normalize-targets
  "Normalizes the `target` key (containing `:root` and `:extension` keys) to
   turn `root/extension` into `system/value' where system is a URI"
  [v]
  (map #(update % :target normalize-id) v))

(defn active-successors
  "Returns the active successor(s) of the given organisation, or the given
  organisation if it is still active"
  ([root id]
   (active-successors (fetch-org root id)))
  ([org]
   (if (:active org)
     [org]
     (flatten (->> (:successors org)
                   (map #(active-successors (get-in % [:target :root]) (get-in % [:target :extension]))))))))

(defn all-predecessors
  "Returns the names of all of the predecessor names of the specified
  organisation"
  ([root id]
   (all-predecessors (fetch-org root id)))
  ([org]
   (concat
     (->> (:predecessors org)
          (map :target)
          (map :extension)
          (map fetch-org)
          (map #(assoc (normalize-id (:orgId %)) :name (:name %))))
     (flatten (->> (:predecessors org)
                   (map #(all-predecessors (get-in % [:target :root]) (get-in % [:target :extension]))))))))

(defn org-identifiers
  "Returns a normalised list of organisation identifiers.
   This turns a single ODS orgId (oid/extension) into a list of uri/values."
  [org]
  [{:system (str "urn:oid:" (get-in org [:orgId :root])) :value (get-in org [:orgId :extension])}
   {:system (get orgRecordClass->namespace (:orgRecordClass org)) :value (get-in org [:orgId :extension])}])

(defn normalize-org
  "Normalizes an organisation, turning legacy ODS OID/extension identifiers into
  namespaced URI/value identifiers"
  [org]
  (if (nil? org)
    nil
    (let [org-type (get orgRecordClass->namespace (:orgRecordClass org))]
      (-> org
          (dissoc :orgId)
          (assoc :identifiers (org-identifiers org)
                 "@type" org-type)
          (update :relationships normalize-targets)
          (update :predecessors normalize-targets)
          (update :successors normalize-targets)))))

(defn org-relationships
  "Turn the list of active relationships in an organisation into a list of
  normalized identifier triples"
  [org]
  (->> (:relationships org)
       (filter :active)
       (map #(hash-map
               :subject {:system (str "urn:oid:" (get-in org [:orgId :root]))
                         :value  (get-in org [:orgId :extension])
                         :name   (:name org)}
               :predicate (let [code (fetch-code (:id %))]
                            {:system (str "urn:oid:" (:codes/code_system code))
                             :value  (:id %)
                             :name   (:codes/display_name code)})
               :object {:system (str "urn:oid:" (get-in % [:target :root]))
                        :value  (get-in % [:target :extension])
                        :name   (:name (fetch-org (get-in % [:target :extension])))}))))

(defn org-succession
  "Turn the successors and predecessors into a list of identifier triples"
  [org k]
  (->> (k org)
       (map #(hash-map
               :subject {:system (str "urn:oid:" (get-in org [:orgId :root]))
                         :value  (get-in org [:orgId :extension])
                         :name   (:name org)}
               :predicate {:system namespace-ods-succession
                           :value  (get {"Predecessor" "resultedFrom"
                                         "Successor"   "resultingOrganization"} (:type %))
                           :date   (:date %)}
               :object {:system (str "urn:oid:" (get-in % [:target :root]))
                        :value  (get-in % [:target :extension])
                        :name   (:name (fetch-org (get-in % [:target :extension])))}))))

(defn organisation-properties
  "Returns a list of organisation properties in triples :subject -> :predicate -> object"
  [org]
  (concat
    (org-relationships org)
    (org-succession org :predecessors)
    (org-succession org :successors)))

(defn http-get-postcode [_ pcode]
  (if-let [pc (fetch-postcode pcode)]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    pc}
    {:status 404}))

(def uri->oid {"2.16.840.1.113883.2.1.3.2.4.18.48"          "2.16.840.1.113883.2.1.3.2.4.18.48"
               namespace-ods-organisation                   "2.16.840.1.113883.2.1.3.2.4.18.48"
               "urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48"  "2.16.840.1.113883.2.1.3.2.4.18.48"
               namespace-ods-site                           "2.16.840.1.113883.2.1.3.2.4.18.48"
               namespace-ods-relationship                   "2.16.840.1.113883.2.1.3.2.4.17.508"
               "urn:oid:2.16.840.1.113883.2.1.3.2.4.17.508" "2.16.840.1.113883.2.1.3.2.4.17.508"})


(pc/defresolver postcode-resolver
  "Resolves a postal code \":postalcode/id\""
  [{:keys [database] :as env} {:keys [:postalcode/id]}]
  {::pc/input  #{:postalcode/id}
   ::pc/output [:organization/id
                :OSGB36/easting :OSGB36/northing
                :LSOA11/id]}
  (if-let [pc (fetch-postcode id)]
    {:organization/id (str namespace-ods-organisation "#" (:PCT pc))
     :OSGB36/easting  (:OSEAST1M pc)
     :OSGB36/northing (:OSNRTH1M pc)
     :LSOA11/id       (:LSOA11 pc)}
    nil))

(pc/defresolver org-resolver
  "Resolves an organisation identifier `:organization/id` made up of uri of
  the form uri#id e.g. \"https://fhir.nhs.uk/Id/ods-organization-code#7A4\".

  A number of different URIs are supported, including OIDS and the FHIR URIs.

  The main idea here is to provide a more abstract and general purpose set of
  properties and relationships for an organisation that that providing by the UK ODS.
  The plan is that the vocabulary should use a standardised vocabulary such as
  that from [https://www.w3.org/TR/vocab-org/](https://www.w3.org/TR/vocab-org/)"
  [{:keys [database] :as env} {:keys [:organization/id]}]
  {::pc/input  #{:organization/id}
   ::pc/output [:organization/identifiers :organization/name :organization/type :organization/active
                :org.w3.www.ns.prov/wasDerivedFrom          ; see https://www.w3.org/TR/prov-o/#wasDerivedFrom
                :organization/isCommissionedBy :organization/subOrganizationOf]}
  (let [[uri value] (str/split id #"#")]
    (when-let [norg (normalize-org (fetch-org (get uri->oid uri) value))]
      {:organization/identifiers          (->> (:identifiers norg)
                                               (map #(str (:system %) "#" (:value %))))
       :organization/name                 (:name norg)
       :organization/type                 (get norg "@type")
       :organization/active               (:active norg)
       :org.w3.www.ns.prov/wasDerivedFrom (->> (:predecessors norg)
                                               (map :target)
                                               (map #(str (:system %) "#" (:value %))))
       :organization/isCommissionedBy     (->> (:relationships norg)
                                               (filter :active)
                                               (filter (fn [rel] (= (:id rel) "RE4")))
                                               (map :target)
                                               (map #(hash-map :organization/id (str (:system %) "#" (:value %)))))
       :organization/subOrganizationOf    (->> (:relationships norg)
                                               (filter (fn [rel] (= (:id rel) "RE6")))
                                               (filter :active)
                                               (map :target)
                                               (map #(hash-map :organization/id (str (:system %) "#" (:value %)))))})))

(pc/defresolver alias-fhir-uk-org
  "An alias to map `:uk.nhs.fhir.id.ods-organization-code/id` into `:organization/id`"
  [{:keys [database] :as env} {:keys [:uk.nhs.fhir.id.ods-organization-code/id]}]
  {::pc/input  #{:uk.nhs.fhir.id.ods-organization-code/id}
   ::pc/output [:organization/id]}
  {:organization/id (str namespace-ods-organisation "#" id)})

(pc/defresolver alias-fhir-uk-site
  "An alias to map `:uk.nhs.fhir.id.ods-site-code/id` into `:organization/id`"
  [{:keys [database] :as env} {:keys [:uk.nhs.fhir.id.ods-site-code/id]}]
  {::pc/input  #{:uk.nhs.fhir.id.ods-site-code/id}
   ::pc/output [:organization/id]}
  {:organization/id (str namespace-ods-site "#" id)})

(def registry
  [postcode-resolver org-resolver alias-fhir-uk-org alias-fhir-uk-site])

(def parser
  (p/parser
    {::p/env     {::p/reader               [p/map-reader
                                            pc/reader2
                                            pc/open-ident-reader
                                            p/env-placeholder-reader]
                  ::p/placeholder-prefixes #{">"}}
     ::p/mutate  pc/mutate
     ::p/plugins [(pc/connect-plugin {::pc/register registry})
                  p/error-handler-plugin
                  p/trace-plugin]}))

(defn http-get-org [uri id]
  (if-let [org (fetch-org (get uri->oid uri) id)]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (let [norg (normalize-org org)]
                (if (:active norg)
                  norg
                  (assoc norg :replacedBy (map normalize-org (active-successors org)))))}
    {:status 404}))

(defn http-get-code [system value]
  (if-let [result (fetch-code value)]
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    {"@type"      system
               :id          (:codes/id result)
               :displayName (:codes/display_name result)
               :codeSystem  (:codes/code_system result)}}
    {:status 404}))

(def resolvers {namespace-ods-organisation                   http-get-org
                namespace-ods-site                           http-get-org
                namespace-ods-relationship                   http-get-code
                namespace-os-postcode                        http-get-postcode
                "urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48"  http-get-org
                "2.16.840.1.113883.2.1.3.2.4.18.48"          http-get-org
                "urn:oid:2.16.840.1.113883.2.1.3.2.4.17.508" http-get-code
                "2.16.840.1.113883.2.1.3.2.4.17.508"         http-get-code})

(defn http-resolve [system value]
  (if-let [resolver (get resolvers system)]
    (resolver system value)
    (route/not-found "Not found")))

(defroutes app-routes

           ;; simple OID resolution for "legacy" clients who want to deal with 'raw' UK ODS identifiers
           (GET "/v1/oid/:root/:id" [root id]
             (http-resolve root id))

           ;; system/value resolution for systems defined as URIs
           (GET "/v1/resolve/:id" [system id]
             (http-resolve system id))

           ;; provide properties as 'triples' (subject, predicate, object); we can only do this for orgs/sites
           (GET "/v1/resolve/:id/properties" [system id]
             (if-let [org (fetch-org (get uri->oid system) id)]
               (organisation-properties org)
               (route/not-found "Not found")))

           ;; get a list of all of the names / identifiers of predecessor organisations
           (GET "/v1/resolve/:id/predecessors" [system id]
             (if-let [uri (get uri->oid system)]
               (all-predecessors uri id)
               (route/not-found "Not found")))

           ;; resolve a postcode
           (GET "/v1/postcode/:postcode" [postcode] (http-get-postcode nil postcode))

           ;; search for an organisation
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

  (def org (fetch-org "7A4"))

  (println (:name org))
  (def org (fetch-org "7A4BV"))
  (def norg (normalize-org org))
  ;; let's try out pathom
  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name :organization/subOrganizationOf]}])

  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name :org.w3.www.ns.prov/wasDerivedFrom]}])

  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name :organization/active :organization/type
                {:organization/subOrganizationOf [:organization/identifiers :organization/name]}]}])
  (def norg (normalize-org (fetch-org "W93036")))
  (parser {} [{[:postalcode/id "CF14 2HB"] [:postalcode/id :organization/name]}])
  (parser {} [{[:postalcode/id "CF14 2HB"] [:OSGB36/easting :OSGB36/northing]}])
  (parser {} [{[:postalcode/id "CF14 4XW"] [:LSOA11/id]}])
  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#W93036"]
               [:organization/name :organization/active :organization/type
                {:organization/isCommissionedBy [:organization/name]}]}])

  (parser {} [{[:uk.nhs.fhir.id.ods-organization-code/id "7A4"]
               [:organization/name :organization/subOrganizationOf]}])

  (parser {} [{[:uk.nhs.fhir.id.ods-site-code/id "7A4BV"]
               [:organization/name :organization/subOrganizationOf]}])

  )
