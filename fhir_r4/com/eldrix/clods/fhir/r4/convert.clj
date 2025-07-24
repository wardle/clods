(ns com.eldrix.clods.fhir.r4.convert
  (:require [com.eldrix.clods.core :as clods]
            [com.eldrix.clods.core :as clods]
            [clojure.string :as str])
  (:import (org.hl7.fhir.r4.model Address Identifier Organization ContactPoint ContactPoint$ContactPointSystem StringType Identifier$IdentifierUse Reference ResourceType Coding CodeableConcept)
           (com.eldrix.clods.core ODS)
           (org.hl7.fhir.r4.model.codesystems OrganizationType)))

(def fhir-system->oid
  {"https://fhir.nhs.uk/Id/ods-organization"   "2.16.840.1.113883.2.1.3.2.4.18.48"
   "https://fhir.nhs.uk/Id/ods-site"           "2.16.840.1.113883.2.1.3.2.4.18.48"
   "urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48" "2.16.840.1.113883.2.1.3.2.4.18.48"
   "2.16.840.1.113883.2.1.3.2.4.18.48"         "2.16.840.1.113883.2.1.3.2.4.18.48"})

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

(def role->r4
  "Mapping of ODS 'role' into FHIR organization 'type'"
  {"RO33"  "govt"                                           ;; county council
   "RO90"  "govt"                                           ;; executive agency
   "RO116" "govt"                                           ;; government agency
   "RO131" "govt"                                           ;; government department site
   "RO141" "govt"                                           ;; local authority
   "RO175" "other"})                                        ;; prison


(defn make-coding [{:keys [codeSystem id displayName]}]
  (doto (Coding.)
    (.setSystem codeSystem)
    (.setCode id)
    (.setDisplay displayName)))

(defn ^CodeableConcept make-organization-type [^ODS ods {:keys [id isPrimary]}]
  (let [role (clods/get-role ods id)
        coding (make-coding (update role :codeSystem #(str "urn:oid:" %)))
        codings (if-not isPrimary
                  [coding]
                  (let [role-code (get role->r4 id "prov")]
                    (conj [coding] (make-coding {:codeSystem  "http://hl7.org/fhir/ValueSet/organization-type"
                                                 :id          role-code
                                                 :displayName (.getDisplay (OrganizationType/fromCode role-code))}))))]
    (doto (CodeableConcept.)
      (.setCoding codings))))

(defn ^ContactPoint make-contact-point [{:keys [type value]}]
  (let [sys (get contact-type->r4 type ContactPoint$ContactPointSystem/OTHER)]
    (doto (ContactPoint.)
      (.setSystem sys)
      (.setValue value))))

(defn make-aliases [ods org]
    (->> (clods/all-equivalent-org-codes ods (get-in org [:orgId :extension]))
         (map (fn [org-code]
                (:name (clods/fetch-org ods org-code))))
         (distinct)
         (map #(StringType. %))))

(defn make-identifier [{:keys [system value type] :or {type :org.hl7.fhir.identifier-use/official}}]
  (doto (Identifier.)
    (.setSystem system)
    (.setValue value)
    (.setUse (Identifier$IdentifierUse/fromCode (name type)))))

(defn make-reference [org]
  (when-let [org-id (first (clods/org-identifiers org))]
    (doto (Reference.)
      (.setType (str ResourceType/Organization))
      (.setIdentifier (make-identifier org-id))
      (.setDisplay (:name org)))))

(defn ^Organization make-organization
  [^ODS ods org]
  (doto (Organization.)
    (.setId ^String (get-in org [:orgId :extension]))
    (.setIdentifier (map make-identifier (clods/org-identifiers org)))
    (.setActive (or (:active org) false))
    (.setPartOf (when-let [[root ext] (clods/org-part-of org)]
                  (make-reference (clods/fetch-org ods root ext))))
    (.setAlias (make-aliases ods org))
    (.setAddress [(make-address org)])
    (.setTelecom (map make-contact-point (:contacts org)))
    (.setType (map (partial make-organization-type ods) (filter :active (:roles org))))
    (.setName (:name org))))

(defn print-fhir [o]
  (let [ctx (ca.uhn.fhir.context.FhirContext/forR4)
        parser (doto (.newJsonParser ctx)
                 (.setPrettyPrint true))]
    (println (.encodeResourceToString parser o))))

(comment
  (def ods (clods/open-index "/var/tmp/ods" "/var/tmp/nhspd-nov-2020"))

  ;; this converts all known organisations into FHIR R4...
  (do (doall (map (partial make-organization ods) (clods/all-organizations ods)))
      (println "done"))

  (def org (make-organization ods (clods/fetch-org ods nil "W93036")))
  (print-fhir org)

  (def org (clods/fetch-org ods nil "X26"))
  (def org (clods/fetch-org ods nil "7A4"))
  org
  (clods/org-identifiers org)
  (map make-identifier (clods/org-identifiers org))

  (def role (clods/get-role ods "RO72"))
  role
  (update role :codeSystem #(str "urn:oid:" %))
  (make-coding role)
  (update role :codeSystem #(str "urn:oid:" %))
  (clods/get-role ods "RO116")

  (clods/get-relationship ods "RE11")
  (clods/code-systems ods)
  (clods/org-part-of org)
  (clods/search-org ods {:s "Castle Gate "}))
