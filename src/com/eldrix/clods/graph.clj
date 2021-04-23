(ns com.eldrix.clods.graph
  "Provides a graph API across UK organisational data."
  (:require [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.core :as clods]
            [com.wsscode.pathom3.connect.operation :as pco]
            [com.wsscode.pathom3.connect.indexes :as pci]
            [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
            [com.wsscode.pathom3.connect.runner :as pcr]
            [com.wsscode.pathom3.interface.eql :as p.eql]
            [com.wsscode.pathom.viz.ws-connector.core :as pvc]
            [com.wsscode.pathom.viz.ws-connector.pathom3 :as p.connector]
            [clojure.string :as str]))

(pco/defresolver uk-org
  "Resolve a UK organisation using the UK ODS.
  Returns data in Organisation Reference Data (ORD) standard."
  [{:keys [clods]} {:urn.oid.2.16.840.1.113883.2.1.3.2.4.18.48/keys [id]}]
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
                 :uk.nhs.ord.location/latlon]}
  (when-let [org (clods/fetch-org clods nil id)]
    {:urn.oid.2.16.840.1.113883.2.1.3.2.4.18.48/id id
     :uk.nhs.ord/name                              (:name org)
     :uk.nhs.ord/active                            (:active org)
     :uk.nhs.ord/orgId                             {:uk.nhs.ord/root                    (get-in org [:orgId :root])
                                                    :uk.nhs.ord/extension               (get-in org [:orgId :extension])
                                                    :uk.nhs.ord./assigningAuthorityName (get-in org [:orgId :assigningAuthorityName])}
     :uk.nhs.ord.operational/start                 (get-in org [:operational :start])
     :uk.nhs.ord.operational/end                   (get-in org [:operational :end])
     :uk.nhs.ord/orgRecordClass                    (:orgRecordClass org)
     :uk.nhs.ord.location/address1                 (get-in org [:location :address1])
     :uk.nhs.ord.location/address2                 (get-in org [:location :address2])
     :uk.nhs.ord.location/town                     (get-in org [:location :town])
     :uk.nhs.ord.location/postcode                 (get-in org [:location :postcode])
     :uk.nhs.ord.location/country                  (get-in org [:location :country])
     :uk.nhs.ord.location/uprn                     (get-in org [:location :uprn])
     :uk.nhs.ord.location/latlon                   (get-in org [:location :latlon])}))

(def all-resolvers
  [uk-org])

(comment
  (def clods (clods/open-index "/var/tmp/ods" "/var/tmp/nhspd"))
  (clods/org-identifiers (clods/fetch-org clods nil "rwmbv"))

  (clods/fetch-postcode clods "cf14 4xw")


  (def registry (-> (pci/register all-resolvers)
                    (assoc :clods clods)))
  (p.connector/connect-env registry {::pvc/parser-id 'clods})


  )