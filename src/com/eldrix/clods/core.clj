(ns com.eldrix.clods.core
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.download :as dl]
            [com.eldrix.clods.sql :as sql]
            [com.eldrix.nhspd.core :as nhspd]
            [geocoordinates.core :as geo])
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
  (if (and (not (and osnrth1m oseast1m)) (and lat lon))
    (let [{:keys [easting northing]} (geo/latitude-longitude->easting-northing {:latitude lat :longitude lon} :national-grid)]
      (assoc from-location :osnrth1m northing :oseast1m easting))
    from-location))

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
    |  |- :lon      : longitude (WGS84), or 
    |  |- :osnrth1m : OSNRTH1M grid ref, 
    |  |- :oseast1m : OSEAST1M grid ref, and
    |  |- :range    : range in metres (optional)
    |- :limit      : limit on number of search results."
  [conn nhspd params]
  (sql/search conn
              (update params :from-location
                      #(-> % (with-coords-from-postcode nhspd) (with-coords-from-wgs84)))))

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

(defprotocol ODS
  (fetch-org [this root extension] [this extension] "Fetch an organisation by identifier")
  (search-org [this params] "Search for an organisation using the parameters specified.")
  (get-role [this id] "Get information about a role")
  (code-systems [this] "Return all ODS codesystems keyed by a vector of codesystem and id."))

(s/def ::root string?)
(s/def ::extension string?)
(s/def ::orgId (s/keys :req-un [::root ::extension]))
(s/def ::ods #(satisfies? ODS %))
(s/def ::ods-dir string?)
(s/def ::f some?)
(s/def ::nhspd #(instance? NHSPD %))
(s/def ::nhspd-dir string?)
(s/def ::open-index-params (s/keys :req-un [::f (or ::nhspd ::nhspd-dir)]
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
  (let [ds (sql/get-ds f)  ;; TODO: could change to a connection pool if required
        managed-nhspd? (not nhspd)                         ;; are we managing nhspd service?
        nhspd (or nhspd (nhspd/open-index nhspd-dir))]
    (reify
      ODS
      (fetch-org [_ root extension]
        (when (or (nil? root) (= root sql/hl7-oid-health-and-social-care-organisation-identifier))
          (sql/fetch-org ds extension)))
      (fetch-org [_ extension]
        (sql/fetch-org ds extension))
      (search-org [_ params]
        (search ds nhspd (merge {:as :ext-orgs} params)))  ;; by default, results are returned as extended orgs
      (code-systems [_] @(delay (sql/codesystems ds)))
      Closeable                                            ;; if we have a connection pool, would close here
      (close [_]                                           ;; if the nhspd service was opened by us, close it.
        (when managed-nhspd? (.close ^Closeable nhspd))))))

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

(comment
  (require '[clojure.spec.test.alpha :as stest])
  (stest/instrument)
  (def ods (open-index {:f "latest-clods.db" :nhspd-dir "../pc4/data/nhspd-2022-11-10.db"}))
  (fetch-org ods "7A4")

  (org-part-of (fetch-org ods "7A4BV"))
  (search-org ods {:s "castle gate" :limit 10})
  (map :name (search-org ods {:roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}}))

;; find surgeries within 2k of Llandaff North, in Cardiff
  (with-open [idx (open-index {:f "latest-clods.db" :nhspd-dir "../pc4/data/nhspd-2022-11-10.db"})]
    (doall (search-org idx {:roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}})))

  (time (with-open [idx (open-index {:f "latest-clods.db" :nhspd-dir "../pc4/data/nhspd-2022-11-10.db"})]
          (doall (map :name (search-org idx {:as :orgs :roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}}))))))


