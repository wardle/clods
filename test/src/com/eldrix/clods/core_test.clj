(ns src.com.eldrix.clods.core-test
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing use-fixtures]]
    [com.eldrix.clods.core :as clods]
    [com.eldrix.clods.graph :as graph]
    [com.eldrix.nhspd.core :as nhspd]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.interface.eql :as p.eql]))

(def ^:dynamic *svc* nil)

(defn trud-api-key
  "Returns the TRUD API key."
  []
  (str/trim-newline (slurp (or (System/getenv "TRUD_API_KEY_FILE") "api-key.txt"))))

(defn live-test-fixture [f]
  (if (and (.exists (io/file "latest-clods.db")) (.exists (io/file "nhspd")))
    (with-open [clods (clods/open-index {:f "latest-clods.db" :nhspd-dir "nhspd"})]
      (println "WARNING: skipping test of install-release as using existing latest-clods.db. Delete this file if required.")
      (binding [*svc* clods]
        (f)))
    (let [api-key (trud-api-key)]
      (println "Installing NHSPD and clods releases")
      (nhspd/write-index "nhspd")
      (with-open [nhspd (nhspd/open-index "nhspd")]
        (clods/install "latest-clods.db" nhspd api-key "cache")
        (with-open [clods (clods/open-index {:f "latest-clods.db" :nhspd nhspd})]
          (binding [*svc* clods]
            (f)))))))

(use-fixtures :once live-test-fixture)

(deftest fetch-org
  (let [cavuhb (clods/fetch-org *svc* nil "7A4")]
    (is (= "7A4" (get-in cavuhb [:orgId :extension])))
    (is (= "CARDIFF & VALE UNIVERSITY LHB" (:name cavuhb)))))

(deftest search-org
  (let [results (clods/search-org *svc* {:as :ext-orgs :s "Cardiff & Vale" :rc "RC1"})]
    (is (= (clods/fetch-org *svc* nil "7A4") (first results)))))

(deftest code-systems
  (is (= (clods/get-role *svc* "RO177")
         {:id          "RO177"
          :code        "177"
          :displayName "PRESCRIBING COST CENTRE"
          :codesystem  "2.16.840.1.113883.2.1.3.2.4.17.507"})))

(deftest relations
  (let [cav-7a4 (clods/related-org-codes *svc* "7A4")
        cav-rwm (clods/related-org-codes *svc* "RWM")
        nearby (clods/search-org *svc* {:as :codes :roles "RO177" :from-location {:postcode "CF14 4XW" :range 5000}})]
    (is (empty? (remove cav-7a4 nearby))
        "All GP surgeries within 5km of 'CF14 4XW' should be related to parent organisation '7A4'.")
    (is (empty? (remove cav-rwm nearby))
        "All GP surgeries within 5km of 'CF14 4XW' should be related to parent organisation 'RWM'.")))

(deftest postcodes
  (let [{:strs [PCD2]} (clods/fetch-postcode *svc* "CF14 4XW")]
    (is (= PCD2 "CF14 4XW")))
  (let [{:strs [PCD2 PCDS]} (clods/fetch-postcode *svc* "b1 2jf")]
    (is (= PCD2 "B1   2JF"))
    (is (= PCDS "B1 2JF"))))

(deftest graph
  (let [env (-> (pci/register graph/all-resolvers) (assoc ::graph/svc *svc*))
        p (partial p.eql/process env)]
    (let [orgs (clods/random-orgs *svc* 1000)]
      (doseq [org orgs]
        (let [{org-name :name, :keys [orgId location]} org
              org# (p {:urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48/id (:extension orgId)}
                      [:urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48/id
                       :uk.nhs.ord/name
                       :org.w3.2004.02.skos.core/prefLabel
                       {:uk.nhs.ord/orgId
                        [:uk.nhs.ord.orgId/root :uk.nhs.ord.orgId/extension]}
                       {:org.hl7.fhir.Organization/address
                        [:org.hl7.fhir.Address/line :org.hl7.fhir.Address/postalCode]}
                       {:uk.nhs.ord/location
                        [:uk.nhs.ord.location/address1 :uk.nhs.ord.location/postcode]}
                       {:org.hl7.fhir.Organization/identifier
                        [:org.hl7.fhir.Identifier/use :org.hl7.fhir.Identifier/system :org.hl7.fhir.Identifier/value]}
                       :org.hl7.fhir.Organization/name])]
          (is (some? org))
          (is (= org-name
                 (:uk.nhs.ord/name org#)
                 (:org.w3.2004.02.skos.core/prefLabel org#)
                 (:org.hl7.fhir.Organization/name org#)))
          (is (= (:root orgId)
                 (get-in org# [:org.hl7.fhir.Organization/identifier 0 :org.hl7.fhir.Identifier/system])
                 (get-in org# [:uk.nhs.ord/orgId :uk.nhs.ord.orgId/root])))
          (is (= (:extension orgId)
                 (:urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48/id org#)
                 (get-in org# [:org.hl7.fhir.Organization/identifier 0 :org.hl7.fhir.Identifier/value])
                 (get-in org# [:uk.nhs.ord/orgId :uk.nhs.ord.orgId/extension])))
          (is (= (:address1 location)
                 (get-in org# [:org.hl7.fhir.Organization/address 0 :org.hl7.fhir.Address/line 0])
                 (get-in org# [:uk.nhs.ord/location :uk.nhs.ord.location/address1])))
          (is (= (:postcode location)
                 (get-in org# [:org.hl7.fhir.Organization/address 0 :org.hl7.fhir.Address/postalCode])
                 (get-in org# [:uk.nhs.ord/location :uk.nhs.ord.location/postcode]))))))))

(comment
  (def *svc* (clods/open-index {:f "latest-clods.db" :nhspd-dir "nhspd"}))
  (def env (-> (pci/register graph/all-resolvers) (assoc ::graph/svc *svc*)))
  (p.eql/process env
                 [{[:urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48/id "7A4BV"]
                   [:urn:oid:2.16.840.1.113883.2.1.3.2.4.18.48/id
                    :uk.nhs.ord/name
                    :org.w3.2004.02.skos.core/prefLabel
                    :uk.nhs.ord/orgId
                    :org.hl7.fhir.Organization/address
                    {:uk.nhs.ord/location
                     [:uk.nhs.ord.location/address1
                      :uk.nhs.ord.location/address2
                      :uk.nhs.ord.location/town
                      :uk.nhs.ord.location/postcode
                      :uk.nhs.ord.location/country]}
                    :uk.nhs.ord/relationships
                    {:uk.nhs.ord/isOperatedBy [:uk.nhs.ord/name]}
                    {:uk.nhs.ord/isPartOf [:uk.nhs.ord/name]}
                    :org.hl7.fhir.Organization/identifier
                    :org.hl7.fhir.Organization/name]}]))
