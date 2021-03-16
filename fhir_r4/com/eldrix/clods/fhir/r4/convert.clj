(ns com.eldrix.clods.fhir.r4.convert
  (:require [com.eldrix.clods.core :as clods])
  (:import (org.hl7.fhir.r4.model Address Identifier Organization ContactPoint ContactPoint$ContactPointSystem StringType Identifier$IdentifierUse)
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

(defn make-identifier [{:keys [system value type] :or {type :org.hl7.fhir.identifier-use/official}}]
  (doto (Identifier.)
    (.setSystem system)
    (.setValue value)
    (.setUse (Identifier$IdentifierUse/fromCode (name type)) )))

(defn ^Organization make-organization
  [^ODS ods org]
  (doto (Organization.)
    (.setId ^String (get-in org [:orgId :extension]))
    (.setIdentifier (map make-identifier (clods/org-identifiers org)))
    (.setActive (:active org))
    (.setAlias (make-aliases ods org))
    (.setAddress [(make-address org)])
    (.setTelecom (map make-contact-point (:contacts org)))
    (.setName (:name org))))

(comment
  (def ods (clods/open-index "/var/tmp/ods" "/var/tmp/nhspd-nov-2020"))

  ;; this converts all known organisations into FHIR R4...
  (do (doall (map make-organization (clods/all-organizations ods)))
      (println "done"))
  (def org (clods/fetch-org ods nil "X26"))
  (clods/org-identifiers org)
  (map make-identifier (clods/org-identifiers org))
  )