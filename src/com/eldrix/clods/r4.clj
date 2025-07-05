(ns com.eldrix.clods.r4
  "Functions to turn ODS data into FHIR R4 data structures."
  (:require [clojure.string :as str]
            [com.eldrix.clods.core :as clods]))

(def fhir-system->oid
  {"https://fhir.nhs.uk/Id/ods-organization"   "2.16.840.1.113883.2.1.3.2.4.18.48"
   "https://fhir.nhs.uk/Id/ods-site"           "2.16.840.1.113883.2.1.3.2.4.18.48"
   "urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48" "2.16.840.1.113883.2.1.3.2.4.18.48"
   "2.16.840.1.113883.2.1.3.2.4.18.48"         "2.16.840.1.113883.2.1.3.2.4.18.48"})

(def contact-type->r4
  {"http" "url"
   "tel" "phone"
   "fax" "fax"})

(defn make-address [{:keys [location]}]
  (let [{:keys [address1 address2 town county country postcode]} location]
    {:use        "work"
     :type       "physical"
     :text       ""
     :line       (remove str/blank? [address1 address2])
     :city       town
     :district   county
     :country    country
     :postalCode postcode}))

(defn make-coding [{:keys [codesystem id displayName]}]
  {:system  codesystem
   :code    id
   :display displayName})

(defn make-org-type-codeable-concept [ods {:keys [id isPrimary]}]
  (when-let [role (clods/get-role ods id)]
    (let [coding (make-coding (update role :codesystem #(str "urn:oid:" %)))
          codings (if-not isPrimary [coding]
                          [coding (make-coding (assoc role :codesystem "http://hl7.org/fhir/ValueSet/organization-type"))])]
      {:coding codings})))

(defn make-contact-point
  [{t :type, value :value}]
  {:system (get contact-type->r4 t "other")
   :value value})

(defn make-aliases
  [ods org]
  (->> (clods/org-code->all-predecessors ods (get-in org [:orgId :extension]) {:as :orgs})
       (map :name)
       (into #{})))

(defn make-identifier
  [{system :system, value :value, t :type :or {t "official"}}]
  {:system system
   :value value
   :use (name t)})


