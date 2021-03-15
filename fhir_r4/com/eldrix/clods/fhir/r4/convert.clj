(ns com.eldrix.clods.fhir.r4.convert
  (:require [com.eldrix.clods.core :as clods]
            [com.eldrix.clods.normalize :as normalize]
            [com.eldrix.clods.index :as index])
  (:import (org.hl7.fhir.r4.model Address Identifier Organization ContactPoint ContactPoint$ContactPointSystem StringType)
           (com.eldrix.clods.core ODS)))

(defn ^Address make-address [org]
  (doto (Address.)
    (.setLine [(StringType. (get-in org [:location :address1]))
               (StringType. (get-in org [:location :address2]))])
    (.setDistrict (get-in org [:location :county]))
    (.setCity (get-in org [:location :town]))
    (.setCountry (get-in org [:location :country]))
    (.setPostalCode (get-in org [:location :postcode]))))

(def contact-type->r4
  {"http" ContactPoint$ContactPointSystem/URL
   "tel"  ContactPoint$ContactPointSystem/PHONE
   "fax"  ContactPoint$ContactPointSystem/FAX})

(defn ^ContactPoint make-contact-point [{:keys [type value]}]
  (let [sys (get contact-type->r4 type ContactPoint$ContactPointSystem/OTHER)]
    (doto (ContactPoint.)
      (.setSystem sys)
      (.setValue value))))

(defn make-aliases [ods org]
  (->> (clods/all-predecessors ods org)
       (map :name)
       (into #{})
       (map #(StringType. %))))

(defn make-identifiers [org]
  (let [root (get-in org [:orgId :root])
        extension (get-in org [:orgId :extension])
        ;; an OID is a legacy HL7 identifier - but the native ODS identifier
        oid (doto (Identifier.)
              (.setSystem root)
              (.setId extension))]
    (cond-> [oid]
            ;; Organisations
            (= :RC1 (:orgRecordClass org))
            (conj (doto (Identifier.) (.setSystem "https://fhir.nhs.uk/Id/ods-organization-code") (.setId extension)))
            ;; Organisation sites
            (= :RC2 (:orgRecordClass org))
            (conj (doto (Identifier.) (.setSystem "https://fhir.nhs.uk/Id/ods-site-code") (.setId extension))))))

(defn ^Organization make-organization
  [^ODS ods org]
  (doto (Organization.)
    (.setId ^String (get-in org [:orgId :extension]))
    (.setIdentifier (make-identifiers org))
    (.setActive (:active org))
    (.setAlias (make-aliases ods org))
    (.setAddress [(make-address org)])
    (.setTelecom (map make-contact-point (:contacts org)))
    (.setName (:name org))))

(comment
  (def ods (clods/open-index "/var/tmp/ods" "/var/tmp/nhspd"))

  ;; this converts all known organisations into FHIR R4...
  (do (doall (map make-organization (clods/all-organizations ods)))
      (println "done"))

  (def org (index/fetch-org searcher nil "X26"))
  (make-organization ods org)
  )