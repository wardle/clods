(ns com.eldrix.clods.graph
  "Provides a graph API across UK organisational data."
  (:require [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.core :as clods]
            [com.wsscode.pathom3.connect.operation :as pco]
            [com.wsscode.pathom3.connect.indexes :as pci]
            [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
            [com.wsscode.pathom3.connect.runner :as pcr]
            [com.wsscode.pathom3.interface.eql :as p.eql]
            [clojure.string :as str]))

(defn target->identifier [{:keys [root extension]}]
  (hash-map (keyword (str "urn.oid." root) "id") extension))

(pco/defresolver uk-org
  "Resolve a UK organisation using the UK ODS.
  Returns data in Organisation Reference Data (ORD) standard."
  [{::keys [svc]} {:urn.oid.2.16.840.1.113883.2.1.3.2.4.18.48/keys [id]}]
  {::pco/output [:urn.oid.2.16.840.1.113883.2.1.3.2.4.18.48/id
                 :uk.nhs.ord/name :uk.nhs.ord/active
                 {:uk.nhs.ord/orgId [:uk.nhs.ord/root
                                     :uk.nhs.ord/extension
                                     :uk.nhs.ord./assigningAuthorityName]}
                 :uk.nhs.ord.operational/start
                 :uk.nhs.ord.operational/end
                 {:uk.nhs.ord/roles [:uk.nhs.ord.role/id :uk.nhs.ord.role/isPrimary :uk.nhs.ord.role/active :uk.nhs.ord.role/startDate :uk.nhs.ord.role/endDate]}
                 :uk.nhs.ord.primaryRole/id :uk.nhs.ord.primaryRole/active :uk.nhs.ord.primaryRole/startDate :uk.nhs.ord.primaryRole/endDate
                 :uk.nhs.ord/orgRecordClass
                 :uk.nhs.ord.location/address1 :uk.nhs.ord.location/address2 :uk.nhs.ord.location/town
                 :uk.nhs.ord.location/postcode :uk.nhs.ord.location/country :uk.nhs.ord.location/uprn
                 :uk.nhs.ord.location/latlon
                 {:uk.nhs.ord/successors [:urn.oid.2.16.840.1.113883.2.1.3.2.4.18.48/id]}
                 {:uk.nhs.ord/predecessors [:urn.oid.2.16.840.1.113883.2.1.3.2.4.18.48/id]}
                 {:uk.nhs.ord/relationships [:uk.nhs.ord.relationship/id
                                             :uk.nhs.ord.relationship/active
                                             :uk.nhs.ord.relationship/startDate
                                             :uk.nhs.ord.relationship/endDate
                                             :uk.nhs.ord.relationship/target
                                             :urn.oid.2.16.840.1.113883.2.1.3.2.4.18.48/id]}]}
  (when-let [org (clods/fetch-org svc nil id)]
    {:urn.oid.2.16.840.1.113883.2.1.3.2.4.18.48/id id
     :uk.nhs.ord/name                              (:name org)
     :uk.nhs.ord/active                            (:active org)
     :uk.nhs.ord/orgId                             {:uk.nhs.ord/root                   (get-in org [:orgId :root])
                                                    :uk.nhs.ord/extension              (get-in org [:orgId :extension])
                                                    :uk.nhs.ord/assigningAuthorityName (get-in org [:orgId :assigningAuthorityName])}
     :uk.nhs.ord.operational/start                 (get-in org [:operational :start])
     :uk.nhs.ord.operational/end                   (get-in org [:operational :end])
     :uk.nhs.ord/orgRecordClass                    (:orgRecordClass org)
     :uk.nhs.ord.location/address1                 (get-in org [:location :address1])
     :uk.nhs.ord.location/address2                 (get-in org [:location :address2])
     :uk.nhs.ord.location/town                     (get-in org [:location :town])
     :uk.nhs.ord.location/postcode                 (get-in org [:location :postcode])
     :uk.nhs.ord.location/country                  (get-in org [:location :country])
     :uk.nhs.ord.location/uprn                     (get-in org [:location :uprn])
     :uk.nhs.ord.location/latlon                   (get-in org [:location :latlon])
     :uk.nhs.ord.primaryRole/id                    (get-in org [:primaryRole :id])
     :uk.nhs.ord.primaryRole/active                (get-in org [:primaryRole :active])
     :uk.nhs.ord.primaryRole/startDate             (get-in org [:primaryRole :startDate])
     :uk.nhs.ord.primaryRole/endDate               (get-in org [:primaryRole :endDate])
     :uk.nhs.ord/roles                             (map (fn [role]
                                                          {:uk.nhs.ord.role/active    (:active role)
                                                           :uk.nhs.ord.role/id        (:id role)
                                                           :uk.nhs.ord.role/isPrimary (:isPrimary role)
                                                           :uk.nhs.ord.role/startDate (:startDate role)
                                                           :uk.nhs.ord.role/endDate   (:endDate role)})
                                                        (:roles org))
     :uk.nhs.ord/relationships                     (map (fn [rel]
                                                          (merge
                                                            {:uk.nhs.ord.relationship/id        (:id rel)
                                                             :uk.nhs.ord.relationship/startDate (:startDate rel)
                                                             :uk.nhs.ord.relationship/endDate   (:endDate rel)
                                                             :uk.nhs.ord.relationship/active    (:active rel)}
                                                            (target->identifier (:target rel)))) (:relationships org))
     :uk.nhs.ord/successors                        (->> (:successors org)
                                                        (map :target)
                                                        (map target->identifier))
     :uk.nhs.ord/predecessors                      (->> (:predecessors org)
                                                        (map :target)
                                                        (map target->identifier))}))


(pco/defresolver skos-preflabel
  [{:uk.nhs.ord/keys [name]}]
  {:org.w3.2004.02.skos.core/prefLabel name})

(pco/defresolver nhspd-pcds
  [{::keys [svc]} {:uk.gov.ons.nhspd/keys [PCDS]}]
  {::pco/output [:uk.gov.ons.nhspd/PCDS
                 :uk.gov.ons.nhspd/PCD2
                 :uk.gov.ons.nhspd/LSOA01
                 :uk.gov.ons.nhspd/LSOA11
                 :uk.gov.ons.nhspd/PCT
                 :uk.gov.ons.nhspd/OSNRTH1M
                 :uk.gov.ons.nhspd/OSEAST1M]}
  (when-let [pc (clods/fetch-postcode svc PCDS)]
    {:uk.gov.ons.nhspd/PCDS     (get pc "PCDS")
     :uk.gov.ons.nhspd/PCD2     (get pc "PCD2")
     :uk.gov.ons.nhspd/LSOA01   (get pc "LSOA01")
     :uk.gov.ons.nhspd/LSOA11   (get pc "LSOA11")
     :uk.gov.ons.nhspd/PCT      (get pc "PCT")
     :uk.gov.ons.nhspd/OSNRTH1M (get pc "OSNRTH1M")
     :uk.gov.ons.nhspd/OSEAST1M (get pc "OSEAST1M")}))

(pco/defresolver org-primary-role-type-resolver
  [{::keys [svc]} {:uk.nhs.ord.primaryRole/keys [id]}]
  {::pco/output [:uk.nhs.ord.primaryRole/displayName
                 :uk.nhs.ord.primaryRole/codeSystem]}
  (when-let [result (get (clods/code-systems svc) ["2.16.840.1.113883.2.1.3.2.4.17.507" id])]
    {:uk.nhs.ord.primaryRole/displayName (:displayName result)
     :uk.nhs.ord.primaryRole/codeSystem  (:codeSystem result)}))

(pco/defresolver org-role-type-resolver
  [{::keys [svc]} {:uk.nhs.ord.role/keys [id]}]
  {::pco/output [:uk.nhs.ord.role/displayName
                 :uk.nhs.ord.role/codeSystem]}
  (when-let [result (get (clods/code-systems svc) ["2.16.840.1.113883.2.1.3.2.4.17.507" id])]
    {:uk.nhs.ord.role/displayName (:displayName result)
     :uk.nhs.ord.role/codeSystem  (:codeSystem result)}))

(pco/defresolver org-rel-type-resolver
  [{::keys [svc]} {:uk.nhs.ord.relationship/keys [id]}]
  {::pco/output [:uk.nhs.ord.relationship/displayName
                 :uk.nhs.ord.relationship/codeSystem]}
  (when-let [result (get (clods/code-systems svc) ["2.16.840.1.113883.2.1.3.2.4.17.508" id])]
    {:uk.nhs.ord.relationship/displayName (:displayName result)
     :uk.nhs.ord.relationship/codeSystem  (:codeSystem result)}))

(pco/defresolver wgs36
  [{:uk.gov.ons.nhspd/keys [OSEAST1M OSNRTH1M]}]
  {::pco/output [:urn.ogc.def.crs.EPSG.4326/latitude
                 :urn.ogc.def.crs.EPSG.4326/longitude]}
  (com.eldrix.nhspd.coords/osgb36->wgs84 OSEAST1M OSNRTH1M))

(def all-resolvers
  "UK ODS resolvers; expect a key :com.eldrix.clods.graph/svc in environment."
  [uk-org
   (pbir/alias-resolver :uk.nhs.ord.location/postcode :uk.gov.ons.nhspd/PCDS)
   (pbir/alias-resolver :uk.gov.ons.nhspd/PCT :urn.oid.2.16.840.1.113883.2.1.3.2.4.18.48/id)
   nhspd-pcds
   org-primary-role-type-resolver
   org-role-type-resolver
   org-rel-type-resolver
   wgs36
   skos-preflabel])


(def role->snomed
  {"RO7"   [284546000 980751000000109]
   "RO76"  [264358009 1101131000000105]
   "RO177" [264358009 1101131000000105]})

(comment
  (def clods (clods/open-index "/var/tmp/ods-2021-04-28" "/var/tmp/nhspd"))
  (def org (clods/fetch-org clods nil "7A3B7"))
  (map (fn [succ] (hash-map (keyword (str "urn:oid:" (get-in succ [:target :root])) "id") (get-in succ [:target :extension]))) (:predecessors org))
  (get (clods/code-systems clods) ["2.16.840.1.113883.2.1.3.2.4.17.507" "RO148"])
  (get (clods/fetch-postcode clods "cf14 4xw") "LSOA11")
  (clods/code-systems clods)
  22232009
  (def registry (-> (pci/register all-resolvers)
                    (assoc ::svc clods)))
  (require '[com.wsscode.pathom.viz.ws-connector.core :as pvc])
  (require '[com.wsscode.pathom.viz.ws-connector.pathom3 :as p.connector])

  (p.connector/connect-env registry {:com.wsscode.pathom.viz.ws-connector.core/parser-id 'clods})

  (uk-org {:clods clods} {:urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48/id "RWM"})
  :urn.ogc.def.crs.EPSG.4326/latitude
  :urn.ogc.def.crs.EPSG.4326/longitude

  (p.eql/process registry
                 [{[:uk.gov.ons.nhspd/PCDS "cf14 4xw"]
                   [:uk.gov.ons.nhspd/LSOA11
                    :uk.gov.ons.nhspd/OSNRTH1M
                    :uk.gov.ons.nhspd/OSEAST1M
                    :uk.gov.ons.nhspd/PCT
                    :uk.nhs.ord/name
                    :uk.nhs.ord.primaryRole/displayName
                    {:uk.nhs.ord/predecessors [:uk.nhs.ord/name]}]}])
  )