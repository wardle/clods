(ns src.com.eldrix.clods.core-test
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing use-fixtures]]
    [com.eldrix.clods.core :as clods]
    [com.eldrix.clods.graph :as graph]
    [com.eldrix.nhspd.api :as nhspd]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.interface.eql :as p.eql]))

(def ^:dynamic *svc* nil)

(defn trud-api-key
  "Returns the TRUD API key."
  []
  (str/trim-newline (slurp (or (System/getenv "TRUD_API_KEY_FILE") "api-key.txt"))))

(def latest-clods-file "latest-clods.db")
(def latest-nhspd-file "latest-nhspd.db")

(defn live-test-fixture [f]
  (let [clods? (.exists (io/file latest-clods-file))
        nhspd? (.exists (io/file latest-nhspd-file))]
    (if (and clods? nhspd?)
      (with-open [clods (clods/open-index {:f latest-clods-file :nhspd-file latest-nhspd-file})]
        (println "WARNING: skipping test of install-release as using existing files:"
                 (str/join " " [latest-clods-file latest-nhspd-file])
                 ". Delete these files if required:")
        (binding [*svc* clods]
          (f)))
      (let [api-key (trud-api-key)]
        (when clods? (.delete (io/file latest-clods-file)))
        (when nhspd? (.delete (io/file latest-nhspd-file)))
        (println "Installing latest clods and nhspd releases for live tests")
        (nhspd/create-latest latest-nhspd-file {:profile :core})
        (with-open [nhspd (nhspd/open latest-nhspd-file)]
          (clods/install latest-clods-file nhspd api-key "cache")
          (with-open [clods (clods/open-index {:f latest-clods-file :nhspd nhspd})]
            (binding [*svc* clods]
              (f))))))))

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
          :codeSystem  "2.16.840.1.113883.2.1.3.2.4.17.507"})))

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

(deftest active-successors
  (let [random-orgs (clods/random-orgs *svc* 100)]
    (doseq [{:keys [active orgId]} random-orgs
            :when active
            :let [active-code (:extension orgId)
                  equiv-codes (clods/equivalent-org-codes *svc* active-code)
                  equiv-orgs (map #(clods/fetch-org *svc* nil %) equiv-codes)
                  inactive-preds (filter #(false? (:active %)) equiv-orgs)]
            :when (seq inactive-preds)]

      (testing (str "inactive predecessors of " active-code " should have it as active successor")
        (doseq [{pred-id :orgId} inactive-preds
                :let [inactive-code (:extension pred-id)
                      active-succ-codes (clods/org-code->active-successors *svc* inactive-code {:as :codes})]]
          (is (contains? active-succ-codes active-code)
              (str "Inactive org " inactive-code " should have active successor " active-code))))

      (testing (str "all active successors are actually active for predecessors of " active-code)
        (doseq [{pred-id :orgId} inactive-preds
                :let [inactive-code (:extension pred-id)
                      active-succ-codes (clods/org-code->active-successors *svc* inactive-code {:as :codes})]]
          (doseq [succ-code active-succ-codes]
            (let [{:keys [active]} (clods/fetch-org *svc* nil succ-code)]
              (is active (str "Successor " succ-code " should be active")))))))))

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
  (def *svc* (clods/open-index {:f latest-clods-file :nhspd-file latest-nhspd-file}))
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
