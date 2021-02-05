(ns com.eldrix.clods.resolve
  (:require [com.eldrix.clods.coords :as coords]
            [com.eldrix.clods.svc :as svc]
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

(def postcode-resolver
  "Resolves a postal code \":postalcode/id\""
  {::pc/sym     'postcode-resolver
   ::pc/input   #{:postalcode/id}
   ::pc/output  [{:uk.gov.ons.nhspd/PCT [:uk.nhs.ods.organisation/orgId]}
                 :uk.gov.ons.nhspd/PCD2                     ;; 8-character version of a UK postcode
                 :uk.gov.ons.nhspd/PCDS                     ;; eGIF standard postcode
                 :uk.gov.ons.nhspd/LSOA11
                 :uk.gov.ons.nhspd/CCG
                 :uk.gov.ons.nhspd/PCT
                 :uk.gov.ons.nhspd/USERTYPE
                 :uk.gov.ons.nhspd/OSNRTH1M
                 :uk.gov.ons.nhspd/OSEAST1M]
   ::pc/resolve (fn [{:keys [svc]} {:postalcode/keys [id]}]
                  (when-let [pc (svc/get-postcode svc id)]
                    {:uk.gov.ons.nhspd/PCD2     (:PCD2 pc)
                     :uk.gov.ons.nhspd/PCDS     (:PCDS pc)
                     :uk.gov.ons.nhspd/PCT      {:uk.nhs.ods.organisation/orgId (:PCT pc)}
                     :uk.gov.ons.nhspd/CCG      {:uk.nhs.ods.organisation/orgId (:CCG pc)}
                     :uk.gov.ons.nhspd/LSOA11   (:LSOA11 pc)
                     :uk.gov.ons.nhspd/USERTYPE (:USERTYPE pc)
                     :uk.gov.ons.nhspd/OSNRTH1M (:OSNRTH1M pc)
                     :uk.gov.ons.nhspd/OSEAST1M (:OSEAST1M pc)
                     :OSGB36/easting            (:OSEAST1M pc)
                     :OSGB36/northing           (:OSNRTH1M pc)
                     :LSOA11/id                 (:LSOA11 pc)}))})

(def wgs84->osgb36-resolver
  "Resolves a latitude/longitude as per WGS84 / EPSG 4326."
  {::pc/sym     'wgs84->osgb36-resolver
   ::pc/input   #{:urn.ogc.def.crs.EPSG.4326/latitude :urn.ogc.def.crs.EPSG.4326/longitude}
   ::pc/output  [:OSGB36/easting :OSGB36/northing]
   ::pc/resolve (fn [_ {:urn.ogc.def.crs.EPSG.4326/keys [latitude longitude]}]
                  (coords/wgs84->osgb36 latitude longitude))})

(def osgb36->wgs84-resolver
  "Resolves a OSGB 36 easting/northing as WGS84 / EPSG4326 latitude/longitude."
  {::pc/sym     'osgb36->wgs84-resolver
   ::pc/input   #{:OSGB36/easting :OSGB36/northing}
   ::pc/output  [:urn.ogc.def.crs.EPSG.4326/latitude :urn.ogc.def.crs.EPSG.4326/longitude]
   ::pc/resolve (fn [_ {:OSGB36/keys [easting northing]}]
                  (coords/osgb36->wgs84 easting northing))})

(def ods-organisation-resolver
  "Resolves an identifier made up of a root/extension of format root#extension.
  This provides a close representation of the ODS data standard."
  {::pc/sym
   'ods-organisation-resolver

   ::pc/input
   #{:uk.nhs.ods.organisation/orgId}

   ::pc/output
   [:uk.nhs.ods.organisation/name
    {:uk.nhs.ods.organisation/location
     [:uk.nhs.ods.organisation.location/town
      :uk.nhs.ods.organisation.location/address1 :uk.nhs.ods.organisation.location/address2
      :uk.nhs.ods.organisation.location/county :uk.nhs.ods.organisation.location/country
      :uk.nhs.ods.organisation.location/postcode]}]

   ::pc/resolve
   (fn [{:keys [svc]} {:uk.nhs.ods.organisation/keys [orgId]}]
     (when-let [org (svc/get-org svc (last (str/split orgId #"#")))]
       #:uk.nhs.ods.organisation{:name     (:name org)
                                 :active   (:active org)
                                 :location #:uk.nhs.ods.organisation.location{:address1 (get-in org [:location :address1])
                                                                              :address2 (get-in org [:location :address2])
                                                                              :town     (get-in org [:location :town])
                                                                              :county   (get-in org [:location :county])
                                                                              :country  (get-in org [:location :country])
                                                                              :postcode (get-in org [:location :postcode])}}))})

(def alias-ods-postcode-nhspd
  (pc/alias-resolver2 :uk.nhs.ods.organisation.location/postcode :postalcode/id))

(def alias-ods-prefName
  (pc/alias-resolver2 :uk.nhs.ods.organisation/name :org.w3.2004.02.skos.core/prefLabel))


(def fhir-v4-organization-resolver
  {::pc/sym
   'fhir-v4-organization-resolver

   ::pc/input
   #{:org.hl7.fhir.Identifier/system :org.hl7.fhir.Identifier/value}

   ::pc/output
   [:org.hl7.fhir.Organization/name
    :org.hl7.fhir.Organization/active]

   ::pc/resolve
   (fn [{:keys [svc]} {:uk.nhs.ods.Identifier/keys [system value]}]
     (when-let [org (svc/get-org svc value)]
       #:org.hl7.fhir.Organization{:name     (:name org)
                                   :active   (:active org)}))})


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
    :postalcode/id
    :org.w3.ns.prov/wasDerivedFrom                          ; see https://www.w3.org/TR/prov-o/#wasDerivedFrom
    :org.w3.2004.02.skos.core/prefLabel
    :organization/isCommissionedBy :organization/subOrganizationOf]

   ::pc/resolve
   (fn [{:keys [svc]} {:organization/keys [id]}]
     (let [[uri value] (str/split id #"#")]
       (when-let [norg (normalize-org (svc/get-org svc value))]
         {:organization/identifiers           (->> (:identifiers norg)
                                                   (map #(str (:system %) "#" (:value %))))
          :organization/name                  (:name norg)
          :org.w3.2004.02.skos.core/prefLabel (:name norg)
          :postalcode/id                      (get-in norg [:location :postcode])
          :organization/type                  (get norg "@type")
          :organization/active                (:active norg)
          :org.w3.ns.prov/wasDerivedFrom      (->> (:predecessors norg)
                                                   (map :target)
                                                   (map #(hash-map :organization/id (str (:system %) "#" (:value %)))))
          :organization/isCommissionedBy      (->> (:relationships norg)
                                                   (filter :active)
                                                   (filter (fn [rel] (= (:id rel) "RE4")))
                                                   (map :target)
                                                   (map #(hash-map :organization/id (str (:system %) "#" (:value %)))))
          :organization/subOrganizationOf     (->> (:relationships norg)
                                                   (filter (fn [rel] (= (:id rel) "RE6")))
                                                   (filter :active)
                                                   (map :target)
                                                   (map #(hash-map :organization/id (str (:system %) "#" (:value %)))))})))})

(def alias-org-suborganization-of
  (pc/alias-resolver2 :organization/subOrganizationOf :org.w3.ns.org/subOrganizationOf))

(def alias-nhspd-easting
  (pc/alias-resolver2 :uk.gov.ons.nhspd/OSEAST1M :OSGB36/easting))

(def alias-nhspd-northing
  (pc/alias-resolver2 :uk.gov.ons.nhspd/OSNRTH1M :OSGB36/northing))

(def alias-fhir-uk-org
  "Resolves a HL7 FHIR namespaced ODS organization code."
  {::pc/sym     `alias-fhir-uk-org
   ::pc/input   #{:uk.nhs.fhir.id/ods-organization-code}
   ::pc/output  [:org.hl7.fhir.Identifier/system :org.hl7.fhir.Identifier/value]
   ::pc/resolve (fn [env {:uk.nhs.fhir.id/keys [ods-organization-code]}]
                  #:org.hl7.fhir.Identifier{:system namespace-ods-organisation
                                           :value ods-organization-code})})

(def alias-fhir-uk-site
  "Resolves a HL7 FHIR namespaced ODS site code."
  {::pc/sym     `alias-fhir-uk-site
   ::pc/input   #{:uk.nhs.fhir.id/ods-site-code}
   ::pc/output  [:organization/id]
   ::pc/resolve (fn [env {:uk.nhs.fhir.id/keys [ods-site-code]}]
                  {:organization/id (str namespace-ods-site "#" ods-site-code)})})

(def registry
  [postcode-resolver
   wgs84->osgb36-resolver
   osgb36->wgs84-resolver
   ods-organisation-resolver
   alias-ods-postcode-nhspd
   fhir-v4-organization-resolver
   org-resolver
   alias-org-suborganization-of
   alias-nhspd-easting
   alias-nhspd-northing
   alias-ods-prefName
   alias-fhir-uk-org alias-fhir-uk-site])

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
  (require '[next.jdbc])
  (def ds (next.jdbc/get-datasource "jdbc:postgresql://localhost/ods"))
  ds
  (require '[com.eldrix.clods.store])
  (def st (com.eldrix.clods.store/open-cached-store ds))
  (svc/get-org st "RWMBV")
  (svc/get-postcode st "NP25 3NS")
  (first (svc/search-org st {:postcode "CF14 2HB" :range-metres 5000}))
  (map normalize-org (active-successors st (svc/get-org st "RWMBV")))

  (def parser (make-parser st))
  ;; let's try out pathom
  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name :organization/subOrganizationOf]}])

  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name :org.w3.ns.prov/wasDerivedFrom]}])

  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name {:org.w3.ns.prov/wasDerivedFrom [:organization/identifiers :organization/name]}]}])

  ;; look up an organisation using a URI (system)/ value identifier and get name and type and suborganisation information
  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#7A4BV"]
               [:organization/name :organization/active :organization/type
                {:organization/subOrganizationOf [:organization/identifiers :organization/name]}]}])

  (def norg (normalize-org (svc/get-org st "W93036")))
  norg
  (parser {} [{[:postalcode/id "CF14 2HB"] [:uk.gov.ons.nhspd/PCDS
                                            :uk.gov.ons.nhspd/PCD2
                                            {:uk.gov.ons.nhspd/PCT [:uk.nhs.ods.organisation/name]}
                                            {:uk.gov.ons.nhspd/CCG [:org.w3.2004.02.skos.core/prefLabel
                                                                    :uk.nhs.ods.organisation/name
                                                                    {:uk.nhs.ods.organisation/location
                                                                     [:uk.nhs.ods.organisation.location/postcode
                                                                      :uk.nhs.ods.organisation.location/address1
                                                                      ;:OSGB36/northing :OSGB36/easting
                                                                      ]}]}
                                            :uk.gov.ons.nhspd/LSOA11
                                            :OSGB36/easting
                                            :OSGB36/northing]}])

  (parser {} [{[:postalcode/id "CF14 2HB"] [:OSGB36/easting :OSGB36/northing :uk.gov.ons.nhspd/PCT]}])
  (parser {} [{[:postalcode/id "CF14 4XW"] [:LSOA11/id]}])
  (parser {} [{[:organization/id "https://fhir.nhs.uk/Id/ods-organization-code#W93036"]
               [:organization/name :organization/active :organization/type
                {:organization/isCommissionedBy [:organization/id :organization/identifiers :organization/name]}]}])

  (parser {} [{[:uk.nhs.fhir.id/ods-organization-code "7A4BV"]
               [:org.hl7.fhir.Identifier/value
                :org.hl7.fhir.Organization/name :organization/subOrganizationOf]}])

  (parser {} [{[:uk.nhs.ods.organisation/orgId "2.16.840.1.113883.2.1.3.2.4.18.48#7A4"]
               [:uk.nhs.ods.organisation/name
                :uk.nhs.ods.organisation.location/postcode]}])

  (parser {} [{[:uk.nhs.fhir.id/ods-site-code "7A4BV"]
               [:org.w3.2004.02.skos.core/prefLabel
                :OSGB36/easting
                :OSGB36/northing
                :org.w3.ns.prov/wasDerivedFrom
                :urn.ogc.def.crs.EPSG.4326/latitude
                :urn.ogc.def.crs.EPSG.4326/longitude
                {:org.w3.ns.org/subOrganizationOf [:org.w3.2004.02.skos.core/prefLabel]}]}])
  )