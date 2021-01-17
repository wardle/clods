(ns com.eldrix.clods.resolve
  (:require [com.eldrix.clods.svc :as svc]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [clojure.string :as str])
  (:import (com.eldrix.clods.svc Store)))

(def namespace-ods-organisation "https://fhir.nhs.uk/Id/ods-organization")
(def namespace-ods-site "https://fhir.nhs.uk/Id/ods-site")
(def namespace-ods-relationship "https://fhir.nhs.uk/Id/ods-relationship")
(def namespace-ods-succession "https://fhir.nhs.uk/Id/ods-succession")
(def namespace-os-postcode "http://data.ordnancesurvey.co.uk/ontology/postcode")

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
  [^Store svc org]
  (if (:active org)
    [org]
    (flatten (->> (:successors org)
                  (map #(active-successors svc (svc/get-org svc (get-in % [:target :extension]))))))))

(defn all-predecessors
  "Returns the names of all of the predecessor names of the specified
  organisation"
  ([^Store svc org]
   (concat
     (->> (:predecessors org)
          (map :target)
          (map :extension)
          (map (partial svc/get-org svc))
          (map #(assoc (normalize-id (:orgId %)) :name (:name %))))
     (flatten (->> (:predecessors org)
                   (map #(all-predecessors svc (get-in % [:target :extension]))))))))

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

(defn fetch-org-relationships
  "Turn the list of active relationships in an organisation into a list of
  normalized identifier triples"
  [^Store svc org]
  (->> (:relationships org)
       (filter :active)
       (map #(hash-map
               :subject {:system (str "urn:oid:" (get-in org [:orgId :root]))
                         :value  (get-in org [:orgId :extension])
                         :name   (:name org)}
               :predicate (let [code (svc/get-code svc (:id %))]
                            {:system (str "urn:oid:" (:codes/code_system code))
                             :value  (:id %)
                             :name   (:codes/display_name code)})
               :object {:system (str "urn:oid:" (get-in % [:target :root]))
                        :value  (get-in % [:target :extension])
                        :name   (:name (svc/get-org svc (get-in % [:target :extension])))}))))

(defn org-succession
  "Turn the successors and predecessors into a list of identifier triples"
  [^Store svc org k]
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
                        :name   (:name (svc/get-org svc (get-in % [:target :extension])))}))))

(defn organisation-properties
  "Convenience function to return a list of organisation properties in triples
      :subject -> :predicate -> object."
  [svc org]
  (concat
    (fetch-org-relationships svc org)
    (org-succession svc org :predecessors)
    (org-succession svc org :successors)))

(def pcode-resolver
  {::pc/sym     `pcode-resolver
   ::pc/input   #{:postalcode/id}
   ::pc/output  [:postalcode/id
                 :organization/id
                 :OSGB36/easting :OSGB36/northing
                 :LSOA11/id]
   ::pc/resolve (fn [{:keys [svc]} {:keys [:postalcode/id]}]
                  (when-let [pc (svc/get-postcode svc id)]
                    {:organization/id (str namespace-ods-organisation "#" (:PCT pc))
                     :OSGB36/easting  (:OSEAST1M pc)
                     :OSGB36/northing (:OSNRTH1M pc)
                     :LSOA11/id       (:LSOA11 pc)}))})

(def postcode-resolver
  "Resolves a postal code \":postalcode/id\""
  {::pc/sym     'postcode-resolver
   ::pc/input   #{:postalcode/id}
   ::pc/output  [:organization/id
                 :OSGB36/easting :OSGB36/northing
                 :LSOA11/id]
   ::pc/resolve (fn [{:keys [svc]} {:postalcode/keys [id]}]
                  (when-let [pc (svc/get-postcode svc id)]
                    {:organization/id (str namespace-ods-organisation "#" (:PCT pc))
                     :OSGB36/easting  (:OSEAST1M pc)
                     :OSGB36/northing (:OSNRTH1M pc)
                     :LSOA11/id       (:LSOA11 pc)}))})

(def org-resolver
  "Resolves an organisation identifier `:organization/id` made up of uri of
  the form uri#id e.g. \"https://fhir.nhs.uk/Id/ods-organization-code#7A4\".

  A number of different URIs are supported, including OIDS and the FHIR URIs.

  The main idea here is to provide a more abstract and general purpose set of
  properties and relationships for an organisation that that provided by the UK ODS.
  The plan is that the vocabulary should use a standardised vocabulary such as
  that from [https://www.w3.org/TR/vocab-org/](https://www.w3.org/TR/vocab-org/)"
  {::pc/sym
   'org-resolver

   ::pc/input
   #{:organization/id}

   ::pc/output
   [:organization/identifiers :organization/name :organization/type :organization/active
    :org.w3.www.ns.prov/wasDerivedFrom                      ; see https://www.w3.org/TR/prov-o/#wasDerivedFrom
    :org.w3.www.2004.02.skos.core/prefLabel
    :organization/isCommissionedBy :organization/subOrganizationOf]

   ::pc/resolve
   (fn [{:keys [svc]} {:organization/keys [id]}]
     (let [[uri value] (str/split id #"#")]
       (when-let [norg (normalize-org (svc/get-org svc value))]
         {:organization/identifiers               (->> (:identifiers norg)
                                                       (map #(str (:system %) "#" (:value %))))
          :organization/name                      (:name norg)
          :org.w3.www.2004.02.skos.core/prefLabel (:name norg)
          :organization/type                      (get norg "@type")
          :organization/active                    (:active norg)
          :org.w3.www.ns.prov/wasDerivedFrom      (->> (:predecessors norg)
                                                       (map :target)
                                                       (map #(hash-map :organization/id (str (:system %) "#" (:value %)))))
          :organization/isCommissionedBy          (->> (:relationships norg)
                                                       (filter :active)
                                                       (filter (fn [rel] (= (:id rel) "RE4")))
                                                       (map :target)
                                                       (map #(hash-map :organization/id (str (:system %) "#" (:value %)))))
          :organization/subOrganizationOf         (->> (:relationships norg)
                                                       (filter (fn [rel] (= (:id rel) "RE6")))
                                                       (filter :active)
                                                       (map :target)
                                                       (map #(hash-map :organization/id (str (:system %) "#" (:value %)))))})))})

(def alias-fhir-uk-org
  "Resolves a HL7 FHIR namespaced ODS organization code."
  {::pc/sym     `alias-fhir-uk-org
   ::pc/input   #{:uk.nhs.fhir.id/ods-organization-code}
   ::pc/output  [:organization/id]
   ::pc/resolve (fn [env {:uk.nhs.fhir.id/keys [ods-organization-code]}]
                  {:organization/id (str namespace-ods-organisation "#" ods-organization-code)})})

(def alias-fhir-uk-site
  "Resolves a HL7 FHIR namespaced ODS site code."
  {::pc/sym     `alias-fhir-uk-site
   ::pc/input   #{:uk.nhs.fhir.id/ods-site-code}
   ::pc/output  [:organization/id]
   ::pc/resolve (fn [env {:uk.nhs.fhir.id/keys [ods-site-code]}]
                  {:organization/id (str namespace-ods-site "#" ods-site-code)})})

(def registry
  [postcode-resolver org-resolver alias-fhir-uk-org alias-fhir-uk-site])

(defn make-parser
  [svc]
  (p/parser
    {::p/env     {::p/reader               [p/map-reader
                                            pc/reader2
                                            pc/open-ident-reader
                                            p/env-placeholder-reader]
                  ::p/placeholder-prefixes #{">"}
                  :svc                     svc}
     ::p/mutate  pc/mutate
     ::p/plugins [(pc/connect-plugin {::pc/register registry})
                  p/error-handler-plugin
                  p/trace-plugin]}))



(comment
  (def ds (next.jdbc/get-datasource "jdbc:postgresql://localhost/ods"))
  ds
  (def st (com.eldrix.clods.store/new-cached-store ds))
  (svc/get-org st "RWMBV")
  (svc/get-postcode st "NP25 3NS")

  (map normalize-org (active-successors st (svc/get-org st "RWMBV")))

  (def parser (make-parser st))
  ;; let's try out pathom
  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name :organization/subOrganizationOf]}])

  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name :org.w3.www.ns.prov/wasDerivedFrom]}])

  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name {:org.w3.www.ns.prov/wasDerivedFrom [:organization/identifiers :organization/name]}]}])

  ;; look up an organisation using a URI (system)/ value identifier and get name and type and suborganisation information
  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name :organization/active :organization/type
                {:organization/subOrganizationOf [:organization/identifiers :organization/name]}]}])

  (def norg (normalize-org (svc/get-org st "W93036")))
  norg
  (parser {} [{[:postalcode/id "CF14 2HB"] [:postalcode/id :organization/name :LSOA11/id :OSGB36/easting :OSGB36/northing]}])
  (parser {} [{[:postalcode/id "CF14 2HB"] [:OSGB36/easting :OSGB36/northing]}])
  (parser {} [{[:postalcode/id "CF14 4XW"] [:LSOA11/id]}])
  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#W93036"]
               [:organization/name :organization/active :organization/type
                {:organization/isCommissionedBy [:organization/id :organization/identifiers :organization/name]}]}])

  (parser {} [{[:uk.nhs.fhir.id/ods-organization-code "7A4"]
               [:organization/name :organization/subOrganizationOf]}])

  (parser {} [{[:uk.nhs.fhir.id/ods-site-code "7A4BV"]
               [:org.w3.www.2004.02.skos.core/prefLabel
                {:organization/subOrganizationOf [:organization/name]}]}])
  )