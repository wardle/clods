(ns com.eldrix.clods.core
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.download :as dl]
            [com.eldrix.clods.sql :as sql]
            [com.eldrix.nhspd.core :as nhspd]
            [geocoordinates.core :as geo]
            [next.jdbc :as jdbc])
  (:import (java.io Closeable)
           (com.eldrix.nhspd.core NHSPD)))

(defn ^:private with-coords-from-postcode
  "Add OS grid references if not already provided in search parameters derived
  from the UK postcode when provided."
  [{:keys [osnrth1m oseast1m postcode] :as from-location} ^NHSPD nhspd]
  (if (or (nil? postcode) (and osnrth1m oseast1m))
    from-location
    (if-let [{:strs [OSNRTH1M OSEAST1M]} (nhspd/fetch-postcode nhspd postcode)]
      (assoc from-location :osnrth1m OSNRTH1M :oseast1m OSEAST1M)
      from-location)))

(defn ^:private with-coords-from-wgs84
  "Add OS grid references if not already in search parameters derived from 
   WGS84 coordinates when provided."
  [{:keys [osnrth1m oseast1m lat lon] :as from-location}]
  (if (and (not (and osnrth1m oseast1m)) lat lon)
    (let [{:keys [easting northing]} (geo/latitude-longitude->easting-northing {:latitude lat :longitude lon} :national-grid)]
      (assoc from-location :osnrth1m northing :oseast1m easting))
    from-location))



;;
;;
;; Public API
;;
;;

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
  - f         : data file; anything coercible via [[clojure.java.io/file]]
  - nhspd     : an NHS postcode directory service
  - api-key   : TRUD api key
  - cache-dir : TRUD cache directory."
  [f ^NHSPD nhspd api-key cache-dir]
  (log/info "Installing NHS organisational data index to:" f)
  (let [ods (download {:api-key api-key :cache-dir cache-dir})]
    (sql/create-db {:f f, :dist ods, :nhspd nhspd}))
  (log/info "Finished creating index at " f))


(defrecord ^:private ODS
  [ds managed-nhspd? ^Closeable nhspd]
  Closeable
  (close [_]
    (when managed-nhspd?
      (.close nhspd))))

(s/def ::root string?)
(s/def ::extension string?)
(s/def ::orgId (s/keys :req-un [::root ::extension]))
(s/def ::ods #(instance? ODS %))
(s/def ::ods-dir string?)
(s/def ::f some?)
(s/def ::nhspd #(instance? NHSPD %))
(s/def ::nhspd-dir string?)
(s/def ::open-index-params
  (s/keys :req-un [::f (or ::nhspd ::nhspd-dir)]
          :opt-un [::ods-dir]))

(defn open-index
  "Open a clods index.
  Parameters are a map containing the following keys:
   - :f         - ODS index file
   - :nhspd     - an already opened NHSPD service
   - :nhspd-dir - directory containing an NHSPD index

  Clods depends upon the NHS Postcode Directory, as provided by <a href=\"https://github.com/wardle/nhspd\">nhspd.
  As such, one of nhspd or nhspd-dir must be provided"
  ^Closeable [{:keys [f ^NHSPD nhspd nhspd-dir] :as params}]
  (when-not (s/valid? ::open-index-params params)
    (throw (ex-info "Cannot open index: invalid parameters" (s/explain-data ::open-index-params params))))
  (let [ds (sql/get-ds f)                                   ;; TODO: could change to a connection pool if required
        managed-nhspd? (not nhspd)                          ;; are we managing nhspd service?
        nhspd (or nhspd (nhspd/open-index nhspd-dir))]
    (->ODS ds managed-nhspd? nhspd)))

(defn valid-service?
  [x]
  (instance? ODS x))

(defn fetch-postcode
  "Fetch raw data from NHSPD.
  For example:
  ```
  (fetch-postcode ods \"cf144xw\")
  =>
  {\"CANNET\" \"N95\",\n \"PCDS\" \"CF14 4XW\",\n \"NHSER\" \"W92\",\n \"SCN\" \"N95\",\n \"PSED\" \"62UBFL16\",
   \"CTRY\" \"W92000004\",\n \"OA01\" \"W00009154\",\n \"HRO\" \"W00\",\n \"OLDHA\" \"QW2\",
   \"RGN\" \"W99999999\",\n \"OSWARD\" \"W05001280\",\n \"LSOA01\" \"W01001770\",\n \"OSNRTH1M\" 179363, ... }
  ```"
  [^ODS ods s]
  (nhspd/fetch-postcode (.-nhspd ods) s))

(defn fetch-org
  ([^ODS ods extension]
   (fetch-org ods nil extension))
  ([^ODS ods root extension]
   (when (or (nil? root) (= root sql/hl7-oid-health-and-social-care-organisation-identifier))
     (sql/fetch-org (.-ds ods) extension))))

(defn random-orgs
  "Return 'n' random organisations. Useful in testing."
  [^ODS ods n]
  (sql/random-orgs (.-ds ods) n))

(defn search-org
  "Search for an organisation
  Parameters :
  - params   : Search parameters; a map containing:
    |- :s             : search for name or address of organisation
    |- :n             : search for name of organisation
    |- :address       : search within address
    |- :fuzzy         : fuzziness factor (0-2)
    |- :only-active?  : only include active organisations (default, true)
    |- :roles         : a string or vector of roles
    |- :primary-role  : a string or vector of roles
    |- :from-location : a map containing:
    |  |- :postcode : UK postal code, or
    |  |- :lat      : latitude (WGS84)
    |  |- :lon      : longitude (WGS84), or
    |  |- :osnrth1m : OSNRTH1M grid ref,
    |  |- :oseast1m : OSEAST1M grid ref, and
    |  |- :range    : range in metres (optional)
    |- :limit      : limit on number of search results."
  [^ODS ods params]
  (sql/search (.-ds ods)
              (-> (merge {:as :ext-orgs} params)            ;; by default, return as 'ext-orgs'
                  (update :from-location (fn [loc] (-> loc  ;; add geocoordinates when needed/possible
                                                       (with-coords-from-postcode (.-nhspd ods))
                                                       (with-coords-from-wgs84)))))))

(defn code-systems
  [ods]
  (sql/codesystems (.-ds ods)))

(def namespace-ods-organisation "https://fhir.nhs.uk/Id/ods-organization")
(def namespace-ods-site "https://fhir.nhs.uk/Id/ods-site")

(def orgRecordClass->namespace
  {:RC1 namespace-ods-organisation
   :RC2 namespace-ods-site})

(defn get-role
  "Return the role associated with code specified, e.g. \"RO72\"."
  [^ODS ods role-code]
  (get (code-systems ods) ["2.16.840.1.113883.2.1.3.2.4.17.507" role-code]))

(defn get-relationship
  "Return the relationship associated with code specified, e.g. \"RE6\""
  [^ODS ods rel-code]
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

(defn org-code->all-predecessors
  [^ODS ods org-code {:keys [as] :or {as :codes} :as opts}]
  (case as
    :codes
    (sql/all-predecessors (.-ds ods) org-code)
    :orgs
    (map #(fetch-org ods %) (sql/all-predecessors (.-ds ods) org-code))
    :ext-orgs
    (map #(sql/extended-org (.-ds ods) (sql/fetch-org (.-ds ods) %)) (sql/all-predecessors (.-ds ods) org-code))
    ;; unsupported 'as' option
    (throw (ex-info "Unsupported return type requested" opts))))

(defn equivalent-org-codes
  "Returns a set of predecessor and successor organisation codes. Set will include
   the original organisation code. Unlike `all-equivalent-orgs` this will *not* return
   the same result and will depend on the starting organisation.
   ```
   (= (equivalent-orgs conn \" RWM \") (equivalent-orgs conn \"7A4\"))
   => false
   ```"
  [^ODS ods org-code]
  (sql/equivalent-orgs (.-ds ods) org-code))

(defn all-equivalent-org-codes
  "Returns a set of equivalent organisation codes by looking at the successors, and
  then returning those and all predecessors. In this way, this returns the same
  result for any organisation within that set.
  ```
  (= (all-equivalent-orgs conn \"RWM\") (all-equivalent-orgs conn \"7A4\"))
  => true
  ```"
  [^ODS ods org-code]
  (sql/all-equivalent-orgs (.-ds ods) org-code))

(defn related-org-codes
  "Return a set of organisation codes for 'related' organisations to the
  specified organisation. This determines related predecessor and successors
  and transitive child organisations.

  For example, to get a set of all current and historic CAVUHB org ids:

  ```
  (take 4 (related-org-codes ods \"7A4\"))
  =>
  (\"VM4A9\" \"RWM4J\" \"VM7WP\" \"RVHQA\")
  ```

  The resulting set can of course be used as a function to determine whether
  another organisation is related:
  ```
  ((related-org-codes ods \"RWM\") \"7A4BV\")    ;; University Hospital Wales
  => \"7A4BV\"

  ((related-org-codes ods \"RWM\") \"7A3B7\")    ;; Princess of Wales Hospital
  => nil
  ```
  This is currently implemented by executing multiple SQL statements, but could
  be refactored to perform as a single SQL statement if required and shown to be
  quicker e.g. using [[search-org]]."
  ([^ODS ods org-code]
   (related-org-codes ods org-code {}))
  ([^ODS ods org-code {:keys [primary-role]}]
   (with-open [conn (jdbc/get-connection (.-ds ods))]
     (let [equiv (sql/equivalent-orgs conn org-code)        ;; set of predecessors and successors
           result (into equiv
                        (mapcat #(sql/all-child-org-codes conn %)) ;; all child organisations
                        equiv)]
       (cond->> result
                ;; filter by primary-role if specified
                primary-role
                (sql/orgs-with-primary-role conn primary-role))))))

(comment
  (require '[clojure.spec.test.alpha :as stest])
  (stest/instrument)
  (def ods (open-index {:f "latest-clods.db" :nhspd-dir "../pc4/data/nhspd-2022-11-10.db"}))
  (time (fetch-org ods "7A4"))
  (get-role ods "RO177")
  (org-code->all-predecessors ods "7A4BV" {:as :orgs})
  (.close ods)
  (org-part-of (fetch-org ods "7A4BV"))
  (search-org ods {:s "castle gate" :limit 10})
  (map :name (search-org ods {:roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}}))


  (time (sql/all-child-org-codes (.-ds ods) "RWM"))
  (time (def org-ids (into #{} (mapcat #(sql/all-child-org-codes (.-ds ods) %))
                           (sql/equivalent-orgs (.-ds ods) "RWM"))))
  (related-org-codes ods "7A4")
  ;; find surgeries within 2k of Llandaff North, in Cardiff
  (with-open [idx (open-index {:f "latest-clods.db" :nhspd-dir "../pc4/data/nhspd-2022-11-10.db"})]
    (doall (search-org idx {:roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}})))

  (time (with-open [idx (open-index {:f "latest-clods.db" :nhspd-dir "../pc4/data/nhspd-2022-11-10.db"})]
          (doall (map :name (search-org idx {:as :orgs :roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}}))))))


