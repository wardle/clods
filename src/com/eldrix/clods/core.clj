(ns com.eldrix.clods.core
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.download :as dl]
            [com.eldrix.clods.index :as index]
            [com.eldrix.nhspd.core :as nhspd])
  (:import (org.apache.lucene.search IndexSearcher)
           (java.io Closeable)
           (com.eldrix.nhspd.core NHSPD)))

(defn download
  "Download the latest ODS release.
  Returns a map containing data including codesystems, codes and the
  organisations.
  See `com.eldrix.clods.download/download`."
  [params]
  (dl/download params))

(defn install
  "Download and install the latest ODS release using the defined NHSPD service.
  Parameters:
  - dir       : directory in which to build ODS service
  - nhspd     : an NHS postcode directory service
  - api-key   : TRUD api key
  - cache-dir : TRUD cache directory."
  [^String dir ^NHSPD nhspd api-key cache-dir]
  (log/info "Installing NHS organisational data index to:" dir)
  (let [ods (download {:api-key api-key :cache-dir cache-dir})]
    (index/install-index nhspd ods dir))
  (log/info "Finished creating index at " dir))

(defn ^:private merge-coords-from-postcode
  "Merge lat/lon information using value of :postcode, if supplied.
  Any existing coordinates will *not* be overwritten by coordinates derived
  from the postal code."
  [^NHSPD nhspd {:keys [postcode] :as loc}]
  (if-not postcode
    loc
    (let [[lat lon] (nhspd/fetch-wgs84 nhspd postcode)]
      (if-not (and lat lon)
        loc
        (merge {:lat lat :lon lon} loc)))))

(defn ^:private search
  "Search for an organisation
  Parameters :
  - searcher : A Lucene IndexSearcher
  - nhspd    : NHS postcode directory service
  - params   : Search parameters; a map containing:
    |- :s             : search for name or address of organisation
    |- :n             : search for name of organisation
    |- :address       : search within address
    |- :fuzzy         : fuzziness factor (0-2)
    |- :only-active?  : only include active organisations (default, true)
    |- :roles         : a string or vector of roles
    |- :from-location : a map containing:
    |  |- :postcode : UK postal code, or
    |  |- :lat      : latitude (WGS84)
    |  |- :lon      : longitude (WGS84), and
    |  |- :range    : range in metres (optional)
    |- :limit      : limit on number of search results."
  [^IndexSearcher searcher ^NHSPD nhspd params]
  (index/search searcher
                (if (get-in params [:from-location :postcode])
                  (update params :from-location (partial merge-coords-from-postcode nhspd))
                  params)))

(defprotocol ODS
  (fetch-org [this root extension] "Fetch an organisation by identifier")
  (search-org [this params] "Search for an organisation using the parameters specified.")
  (child-orgs [this params])
  (all-organizations [this] "Returns a lazy sequence of all organisations")
  (code-systems [this] "Return all ODS codesystems")
  (fetch-postcode [this pc] "Return NHSPD data about the specified postcode.")
  (fetch-wgs84 [this pc] "Returns WGS84 lat/long coordinates about the postcode."))


(s/def ::root string?)
(s/def ::extension string?)
(s/def ::orgId (s/keys :req-un [::root ::extension]))
(s/def ::ods #(satisfies? ODS %))
(s/def ::ods-dir string?)
(s/def ::nhspd #(instance? NHSPD %))
(s/def ::nhspd-dir string?)
(s/def ::open-index-params (s/keys :req-un [::ods-dir (or ::nhspd ::nhspd-dir)]))

(s/fdef open-index
  :args (s/alt :vec (s/cat :ods-dir string? :nhspd-dir string?)
               :map (s/keys :req-un [::ods-dir (or ::nhspd ::nhspd-dir)])))
(defn open-index
  "Open a clods index.
  Parameters are a map containing the following keys:
   - :ods-dir   - directory representing the ODS index
   - :nhspd     - an already opened NHSPD service
   - :nhspd-dir - directory containing an NHSPD index

  Clods depends upon the NHS Postcode Directory, as provided by <a href=\"https://github.com/wardle/nhspd\">nhspd.
  As such, one of nhspd or nhspd-dir must be provided"
  ^:deprecated ([ods-dir nhspd-dir] (open-index {:ods-dir ods-dir :nhspd-dir nhspd-dir}))
  ([{:keys [ods-dir ^NHSPD nhspd nhspd-dir] :as params}]
   (when-not (s/valid? ::open-index-params params)
     (throw (ex-info "Cannot open index: invalid parameters" (s/explain-data ::open-index-params params))))
   (let [reader (index/open-index-reader ods-dir)
         searcher (IndexSearcher. reader)
         managed-nhspd? (not nhspd)                         ;; are we managing nhspd service?
         nhspd (or nhspd (nhspd/open-index nhspd-dir))
         code-systems (index/read-metadata searcher "code-systems")]
     (reify
       ODS
       (fetch-org [_ root extension] (index/fetch-org searcher root extension))
       (search-org [_ params] (search searcher nhspd params))
       (child-orgs [_ params] (index/child-relationships searcher params))
       (all-organizations [_] (index/all-organizations reader))
       (code-systems [_] code-systems)
       (fetch-postcode [_ pc] (nhspd/fetch-postcode nhspd pc))
       (fetch-wgs84 [_ pc] (nhspd/fetch-wgs84 nhspd pc))
       Closeable
       (close [_]                                           ;; if the nhspd service was opened by us, close it.
         (.close reader)
         (when managed-nhspd? (.close nhspd)))))))


(def namespace-ods-organisation "https://fhir.nhs.uk/Id/ods-organization")
(def namespace-ods-site "https://fhir.nhs.uk/Id/ods-site")
(def orgRecordClass->namespace {:RC1 namespace-ods-organisation
                                :RC2 namespace-ods-site})

(defn get-role
  "Return the role associated with code specified, e.g. \"RO72\"."
  [ods role-code]
  (get (code-systems ods) ["2.16.840.1.113883.2.1.3.2.4.17.507" role-code]))

(defn get-relationship
  "Return the relationship associated with code specified, e.g. \"RE6\""
  [ods rel-code]
  (get (code-systems ods) ["2.16.840.1.113883.2.1.3.2.4.17.508" rel-code]))

(def re-org-id #"^((?<root>.*?)\|)?(?<extension>.*?)$")

(s/fdef parse-org-id
  :args (s/cat :s string?))
(defn parse-org-id [s]
  (when-not (str/blank? s)
    (let [[_ _ root extension] (re-matches re-org-id s)]
      {:root      root
       :extension extension})))

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
  "Returns the active successor(s) of the given organisation.
  If the specified organisation is still active, by default returns a sequence
  containing only it. If 'self-if-active?' is false, returns nil. "
  [ods org & {:keys [self-if-active?] :or {self-if-active? true}}]
  (if (:active org)
    (when self-if-active? [org])
    (flatten (->> (:successors org)
                  (map #(active-successors ods (fetch-org ods nil (get-in % [:target :extension]))))))))

(defn predecessors
  "Returns a lazy sequence of direct predecessor organisations."
  [ods org]
  (->> (:predecessors org)
       (map #(fetch-org ods (get-in % [:target :root]) (get-in % [:target :extension])))))

(defn all-predecessors
  "Returns a vector of all predecessor organisations."
  [ods org]
  (loop [remaining-orgs (vec (predecessors ods org))
         result []]
    (if-not (seq remaining-orgs)
      result
      (let [org' (first remaining-orgs)
            remaining (or (next remaining-orgs) [])]
        (recur (into remaining (predecessors ods org'))
               (conj result org'))))))

(comment
  (all-predecessors idx (fetch-org idx nil "7A4BV")))

(defn org-identifiers
  "Returns a normalised list of organisation identifiers.
  The first will be the 'best' identifier to use for official use.
  This turns a single ODS orgId (oid/extension) into a list of uri/values."
  [org]
  [{:system (get orgRecordClass->namespace (:orgRecordClass org)) :value (get-in org [:orgId :extension]) :type :org.hl7.fhir.identifier-use/official}
   {:system (str "urn:oid:" (get-in org [:orgId :root])) :value (get-in org [:orgId :extension]) :type :org.hl7.fhir.identifier-use/old}])

(def part-of-relationships
  "A priority list of what relationship to use in order to
  determine the more abstract 'part-of' relationship."
  {"RE2" 1                                                  ;; is a subdivision
   "RE3" 2                                                  ;; is directed by
   "RE6" 3                                                  ;; is operated by
   "RE4" 4})                                                ;; is commissioned by


(defn org-part-of
  "Returns a best-match of what we consider an organisation 'part-of'.
  Returns a tuple of root extension."
  [org]
  (let [rel (->> (:relationships org)
                 (map #(assoc % :priority (get part-of-relationships (:id %))))
                 (filter :active)
                 (filter :priority)
                 (sort-by :priority)
                 first)]
    (when rel
      [(get-in rel [:target :root]) (get-in rel [:target :extension])])))

(defn normalize-org
  "Normalizes an organisation, turning legacy ODS OID/extension identifiers into
  namespaced URI/value identifiers"
  [org]
  (when org
    (let [org-type (get orgRecordClass->namespace (:orgRecordClass org))]
      (-> org
          (dissoc :orgId)
          (assoc :identifiers (org-identifiers org)
                 "@type" org-type)
          (update :relationships normalize-targets)
          (update :predecessors normalize-targets)
          (update :successors normalize-targets)))))

(s/fdef matching-org-id?
  :args (s/cat :source ::orgId
               :target (s/alt :target-org-id ::orgId
                              :target-extension string?)))
(defn matching-org-id?
  "Do the organisational identifiers match?
  Parameters:
  - source-org-id   : a map containing ':root' and ':extension' keys
  - target-org-id   : one of:
                      - a map containing ':root' and ':extension' keys
                      - a string representing the extension."
  [source-org-id target-org-id]
  (or (and (= (:root source-org-id) (:root target-org-id)) (= (:extension source-org-id) (:extension target-org-id)))
      (= source-org-id target-org-id)
      (and (string? target-org-id) (= (:extension source-org-id) (str/upper-case target-org-id)))))

(defn related?
  "Is the organisation specified related to the target specified?
  The target should be the organisation, or a parent of that organisation, or
  have a similar historical relationship.

  Parameters:
  - ods           : ods index
  - org           : a map representing the organisation, from 'fetch-org'
  - target        : a map representing the organisation, from 'fetch-org'
  - rels          : (default, 'all') a set of relationship types, or predicate
  - historic?     : (default, true) whether to use historic relationships

  For example,
  ```
    (related? ods (fetch-org ods nil \"RWMBV\") (fetch-org ods nil \"7A4\"))
  ```
  returns a truthy value, as 'RWMBV' was the old University Hospital of Wales,
  under a parent organisation, 'RWM', which is now inactive and replaced with
  '7A4'."
  [ods org target & {:keys [rels historic?] :or {historic? true} :as opts}]
  (when org
    (or (matching-org-id? (:orgId org) (:orgId target))     ;; shortcut if they are the same
        (let [org-rels (if rels (filter #(rels (:id %)) (:relationships org)) (:relationships org))]
          (or
            ;; perhaps one of the relationships match directly?
            (some #(matching-org-id? (:target %) (:orgId target)) org-rels)
            ;; or recurse through source organisation relationships if there's not a direct match
            (some #(related? ods (fetch-org ods (get-in % [:target :root]) (get-in % [:target :extension])) target) org-rels)
            ;; do a comparison of active successors, if we're using historic relationships and either org is inactive...
            (and historic? (or (not (:active org)) (not (:active target)))
                 (let [org-succs (active-successors ods org)
                       target-succs (active-successors ods target)
                       test-seq (for [s org-succs t target-succs] [s t])]
                   (some (fn [[s t]] (related? ods s t :rels rels :historic? false)) test-seq))))))))

(defn org->id
  "Turn an organisation into a tuple of root and extension."
  [{:keys [orgId]}]
  (vector (:root orgId) (:extension orgId)))

(defn equivalent-org-ids
  "Return a set of all successor and predecessor organisational identifiers.
  Example:
  ```
  (equivalent-org-ids ods {:extension \"7a4bv\"})
  =>
  #{[\"2.16.840.1.113883.2.1.3.2.4.18.48\" \"7A4BV\"]
    [\"2.16.840.1.113883.2.1.3.2.4.18.48\" \"RVGBV\"]
    [\"2.16.840.1.113883.2.1.3.2.4.18.48\" \"WH2BV\"]
    [\"2.16.840.1.113883.2.1.3.2.4.18.48\" \"RRBBV\"]
    [\"2.16.840.1.113883.2.1.3.2.4.18.48\" \"RWMBV\"]}"
  [ods {:keys [org orgId root extension]}]
  (let [org (or org (if orgId (fetch-org ods (:root orgId) (:extension orgId)) (fetch-org ods root extension)))
        orgs (active-successors ods org)                    ;; all successors
        orgs-id (into #{} (map org->id) orgs)
        predecessors (mapcat #(all-predecessors ods %) orgs)] ;; get all predecessors of all successors and possibly children
    (into orgs-id (map org->id) predecessors)))

(defn equivalent-and-child-org-ids
  "Given a root and extension, returns a set of tuples of root and extensions
  representing successor and predecessor organisations as well as 'child'
  organisations.
  For example, to get all Cardiff and Vale UHB organisational identifiers:
  ```
  (into #{} (map second) (equivalent-org-ids-and-children ods nil \"7A4\"))
  =>
  #{\"RRA\"\n  \"V08122\"\n  \"W00124\"\n  \"7A44A\"\n  \"W97286\"\n  \"W97619\"\n  \"RWM\" ... }
  ```
  "
  [ods root extension]
  (let [equivalent (equivalent-org-ids ods {:root root :extension extension})
        children (map org->id (mapcat (fn [[root extension]] (child-orgs ods {:root root :extension extension})) equivalent))]
    (into equivalent children)))

(comment
  (require '[clojure.spec.test.alpha :as stest])
  (stest/instrument)
  (def ods (open-index {:ods-dir "ods-2024-01-08.db" :nhspd-dir "../pc4/data/nhspd-2022-11-10.db"}))
  (def ods (open-index {:ods-dir "../pc4/data/ods-2022-01-24.db" :nhspd-dir "../pc4/data/nhspd-2022-11-10.db"}))
  (active-successors ods (fetch-org ods nil "RWM"))
  (all-predecessors ods (fetch-org ods nil "7A4BV"))
  (active-successors ods (fetch-org ods nil "7A4BV"))
  (fetch-org ods nil "7A4")
  (sort (map (fn [[r e]] (str e ":" (:name (fetch-org ods r e)))) (equivalent-org-ids-and-children ods nil "7a4")))
  (= (equivalent-org-ids-and-children ods nil "RWM")
     (equivalent-org-ids-and-children ods nil "7A4"))
  (time (let [extensions (into #{} (map second) (equivalent-org-ids ods {:extension "7A4"}))
              children (map #(get-in % [:orgId :extension]) (mapcat #(child-orgs ods {:extension %}) extensions))]
          (into extensions children)))
  (->> (equivalent-org-ids ods {:extension "RWM"})
       (map second)
       (map #(fetch-org ods nil %))
       (mapcat #(child-orgs ods {:org %}))
       (map :name))
  (related? ods (fetch-org ods nil "RVFAR") (fetch-org ods nil "7A4"))
  (related? ods (fetch-org ods nil "RWMBV") (fetch-org ods nil "7A4"))
  (fetch-org ods nil "RWM")
  (search-org ods {:s "castle gate" :limit 10})
  (search-org ods {:roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}})
  (with-open [idx (open-index "/var/tmp/ods" "/var/tmp/nhspd")]
    (fetch-org idx nil "RWMBV"))

  (with-open [idx (open-index "/var/tmp/ods" "/var/tmp/nhspd")]
    (doall (search-org idx {:s "vale" :limit 1})))

  (with-open [idx (open-index "/var/tmp/ods" "/var/tmp/nhspd")]
    (doall (search-org idx {:s "vale" :limit 2 :from-location {:postcode "CF14 4XW"}})))

  ;; find surgeries within 2k of Llandaff North, in Cardiff
  (with-open [idx (open-index "/var/tmp/ods" "/var/tmp/nhspd")]
    (doall (search-org idx {:roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}})))

  (with-open [idx (open-index "/var/tmp/ods" "/var/tmp/nhspd")]
    (doall (search-org idx {:roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}}))))


