(ns com.eldrix.clods.index
  "An NHS Organisation Directory Service (ODS) file-based index providing 
   UK organisational search functionality."
  (:require [clojure.core.async :as a]
            [clojure.string :as str]
            [com.eldrix.nhspd.core :as nhspd]
            [taoensso.nippy :as nippy])
  (:import (org.apache.lucene.index Term IndexWriter IndexWriterConfig DirectoryReader IndexWriterConfig$OpenMode IndexReader)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.document Document Field$Store StoredField TextField StringField LatLonPoint LatLonDocValuesField)
           (org.apache.lucene.search IndexSearcher TermQuery TopDocs ScoreDoc BooleanQuery$Builder BooleanClause$Occur Query PrefixQuery FuzzyQuery Sort SortField)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (java.nio.file Paths)
           (org.apache.lucene.analysis.tokenattributes CharTermAttribute)
           (org.apache.lucene.analysis Analyzer)
           (com.eldrix.nhspd.core NHSPD)))

(set! *warn-on-reflection* true)

(def index-version 2)

(def ^String hl7-oid-health-and-social-care-organisation-identifier
  "The default organisation root is the HL7 OID representing a
  HealthAndSocialCareOrganisationIdentifier"
  "2.16.840.1.113883.2.1.3.2.4.18.48")

(defn org-id->str
  ^String [{:keys [root extension]}]
  (str root "#" extension))

(defn make-org-id
  ^String [{:keys [orgId]}]
  (org-id->str orgId))

(defn make-organisation-doc
  "Turn an organisation into a Lucene document.
  At the moment, we use postcode to derive WGS84 coordinates (lat/lon) for an
  organisation. In the future, when ODS contains UPRNs for organisations, we
  could use UPRN to derive geographical coordinates."
  [^NHSPD nhspd org]
  (let [[lat long] (nhspd/fetch-wgs84 nhspd (get-in org [:location :postcode]))
        org' (if (and lat long) (assoc-in org [:location :latlon] [lat long]) org)
        doc (doto (Document.)
              (.add (StringField. "id" (make-org-id org) Field$Store/NO))
              (.add (StoredField. "data" ^bytes (nippy/freeze org')))
              (.add (StringField. "root" ^String (get-in org [:orgId :root]) Field$Store/NO))
              (.add (StringField. "extension" ^String (get-in org [:orgId :extension]) Field$Store/NO))
              (.add (TextField. "name" (:name org) Field$Store/YES))
              (.add (StringField. "active" (str (:active org)) Field$Store/NO))
              (.add (TextField. "address"
                                (str/join " " [(get-in org [:location :address1])
                                               (get-in org [:location :address2])
                                               (get-in org [:location :town])
                                               (get-in org [:location :county])
                                               (get-in org [:location :postcode])
                                               (get-in org [:location :country])]) Field$Store/NO)))]
    (when (and lat long)
      (.add doc (LatLonPoint. "latlon" lat long))
      (.add doc (LatLonDocValuesField. "latlon" lat long)))
    (doseq [role (:roles org)]
      (when (:active role) (.add doc (StringField. "role" ^String (:id role) Field$Store/NO))))
    (doseq [rel (:relationships org)]
      (when (:active rel)
        (.add doc (StringField. ^String (:id rel) (org-id->str (:target rel)) Field$Store/NO))))
    doc))

(defn write-batch!
  [^IndexWriter writer nhspd orgs]
  (dorun (map (fn [org] (.updateDocument writer (Term. "id" (make-org-id org)) (make-organisation-doc nhspd org))) orgs))
  (.commit writer))

(defn make-metadata-document
  "Create a Lucene document to store arbitrary data using the key specified."
  [^String k m]
  (doto (Document.)
    (.add (StringField. "metadata" k Field$Store/NO))
    (.add (StoredField. "data" ^bytes (nippy/freeze m)))))

(defn write-metadata
  [^IndexWriter writer ^String k m]
  (.updateDocument writer (Term. "metadata" k) (make-metadata-document k m))
  (.commit writer))

(defn read-metadata
  [^IndexSearcher searcher ^String k]
  (when-let [score-doc ^ScoreDoc (first (seq (.-scoreDocs ^TopDocs (.search searcher (TermQuery. (Term. "metadata" k)) 1))))]
    (when-let [doc (.doc searcher (.-doc score-doc))]
      (nippy/thaw (.-bytes (.getBinaryValue doc "data"))))))

(defn open-index-writer
  ^IndexWriter [filename]
  (let [analyzer (StandardAnalyzer.)
        directory (FSDirectory/open (Paths/get filename (into-array String [])))
        writer-config (doto (IndexWriterConfig. analyzer)
                        (.setOpenMode IndexWriterConfig$OpenMode/CREATE_OR_APPEND))]
    (IndexWriter. directory writer-config)))

(defn build-index
  "Build an index from NHS ODS data streamed on the channel specified."
  [^NHSPD nhspd ch out]
  (with-open [writer (open-index-writer out)]
    (a/<!!                                                  ;; block until pipeline complete
      (a/pipeline                                           ;; pipeline for side-effects
        (.availableProcessors (Runtime/getRuntime))         ;; parallelism factor
        (doto (a/chan) (a/close!))                          ;; output channel - /dev/null
        (map (partial write-batch! writer nhspd))
        ch))
    (.forceMerge writer 1)))

(defn install-index
  "Install an index into the directory 'out' using the data provided."
  [^NHSPD nhspd {:keys [organisations code-systems]} out]
  (build-index nhspd organisations out)
  (with-open [writer (open-index-writer out)]
    (write-metadata writer "code-systems" code-systems)
    (.forceMerge writer 1)))

(defn open-index-reader
  ^IndexReader [filename]
  (let [directory (FSDirectory/open (Paths/get filename (into-array String [])))]
    (DirectoryReader/open directory)))

(defn q-orgId
  "Make a query for the identifier specified.
  - root      : (optional) root OID
  - extension : organisation extension (code)."
  (^Query [^String extension]
   (q-orgId nil extension))
  (^Query [^String root ^String extension]
   (-> (BooleanQuery$Builder.)
       (.add (TermQuery. (Term. "root" ^String (or root hl7-oid-health-and-social-care-organisation-identifier))) BooleanClause$Occur/MUST)
       (.add (TermQuery. (Term. "extension" (str/upper-case extension))) BooleanClause$Occur/MUST)
       (.build))))

(defn do-query [^IndexSearcher searcher ^Query q ^long max-hits]
  (let [results (seq (.-scoreDocs ^TopDocs (.search searcher q max-hits)))]
    (map #(.doc searcher (.-doc ^ScoreDoc %)) results)))

(defn doc->organisation
  "Deserialize a Lucene document into an ODS organisation."
  [^Document doc]
  (nippy/thaw (.-bytes (.getBinaryValue doc "data"))))

(defn fetch-org
  "Returns NHS ODS data for the organisation specified.
  Parameters:
   - searcher  : Lucene IndexSearcher
   - root      : (optional) the identifier root;
                 default '2.16.840.1.113883.2.1.3.2.4.18.48'
   - extension : organisation code; eg. '7A4BV'."
  ([^IndexSearcher searcher extension]
   (fetch-org searcher hl7-oid-health-and-social-care-organisation-identifier extension))
  ([^IndexSearcher searcher root extension]
   (when-not (str/blank? extension)
     (when-let [score-doc ^ScoreDoc (first (seq (.-scoreDocs ^TopDocs (.search searcher (q-orgId root extension) 1))))]
       (when-let [doc (.doc searcher (.-doc score-doc))]
         (doc->organisation doc))))))


(defn all-organizations
  "Returns a lazy sequence of all organisations.
   The reader must remain open until the sequence is fully realised."
  [^IndexReader reader]
  (->> (range (.maxDoc reader))
       (map #(.document reader %))
       (remove nil?)
       (map doc->organisation)))

(defn all-contact-types
  "Given a sequence of organisations, return all used contact types."
  [orgs]
  (->> orgs
       (map :contacts)
       (flatten)
       (map :type)
       (into #{})))

(defn do-raw-query
  ([^IndexSearcher searcher ^Query q max-hits ^Sort sort]
   (map #(.doc searcher (.-doc ^ScoreDoc %)) (seq (.-scoreDocs ^TopDocs (.search searcher q ^int max-hits sort)))))
  ([^IndexSearcher searcher ^Query q max-hits]
   (map #(.doc searcher (.-doc ^ScoreDoc %)) (seq (.-scoreDocs ^TopDocs (.search searcher q ^int max-hits))))))

(defn q-or
  [queries]
  (case (count queries)
    0 nil
    1 (first queries)
    (let [builder (BooleanQuery$Builder.)]
      (doseq [^Query query queries]
        (.add builder query BooleanClause$Occur/SHOULD))
      (.build builder))))

(defn q-and
  [queries]
  (case (count queries)
    0 nil
    1 (first queries)
    (let [builder (BooleanQuery$Builder.)]
      (doseq [query queries]
        (.add builder ^Query query BooleanClause$Occur/MUST))
      (.build builder))))

(defn- q-token
  "Creates a query on the named field using the token specified."
  [^String field-name ^String token fuzzy]
  (let [term (Term. field-name token)
        tq (TermQuery. term)
        builder (BooleanQuery$Builder.)]
    (.add builder (PrefixQuery. term) BooleanClause$Occur/SHOULD)
    (if (and fuzzy (> fuzzy 0)) (.add builder (FuzzyQuery. term (min 2 fuzzy)) BooleanClause$Occur/SHOULD)
                                (.add builder tq BooleanClause$Occur/SHOULD))
    (.setMinimumNumberShouldMatch builder 1)
    (.build builder)))

(defn tokenize
  "Tokenize the string 's' according the 'analyzer' and field specified."
  [^Analyzer analyzer ^String field-name ^String s]
  (with-open [tokenStream (.tokenStream analyzer field-name s)]
    (let [termAtt (.addAttribute tokenStream CharTermAttribute)]
      (.reset tokenStream)
      (loop [has-more (.incrementToken tokenStream)
             result []]
        (if-not has-more
          result
          (let [term (.toString termAtt)]
            (recur (.incrementToken tokenStream) (conj result term))))))))

(defn- q-tokens
  "Creates a query for field specified using the string specified."
  ([field-name s] (q-tokens field-name s 0))
  ([field-name s fuzzy]
   (with-open [analyzer (StandardAnalyzer.)]
     (when s (q-and (map #(q-token field-name % fuzzy) (tokenize analyzer field-name s)))))))

(defn q-name
  ([s] (q-name s 0))
  ([s fuzzy]
   (q-tokens "name" s fuzzy)))

(defn q-address
  ([s] (q-address s 0))
  ([s fuzzy]
   (q-tokens "address" s fuzzy)))

(defn sort-by-distance
  "Creates an Apache Lucene 'Sort' based on distance from the location given."
  ([[lat lon]] (sort-by-distance lat lon))
  ([lat lon]
   (Sort. (into-array ^SortField [(LatLonDocValuesField/newDistanceSort "latlon" lat lon)]))))

(defn q-location
  "Creates a query for an organisation within 'distance' metres of the
  location specified."
  ([[lat lon] distance] (q-location lat lon distance))
  ([lat lon distance]
   (LatLonPoint/newDistanceQuery "latlon" lat lon distance)))

(defn q-active
  "A query to limit to active organisations"
  []
  (TermQuery. (Term. "active" "true")))

(defn q-roles
  "Query for the role(s) specified. "
  [roles]
  (cond
    (string? roles)
    (TermQuery. (Term. "role" ^String roles))
    (and (coll? roles) (= 1 (count roles)))
    (TermQuery. (Term. "role" ^String (first roles)))
    :else
    (let [builder (BooleanQuery$Builder.)]
      (doseq [role roles]
        (.add builder (TermQuery. (Term. "role" ^String role)) BooleanClause$Occur/SHOULD))
      (.build builder))))

(defn q-rel
  "Query for organisations with a relationship *to* the specified organisation.
  As ODS only stores parent relationships ie child org (x) has a parent org (y),
  this query essentially helps traverse from parent to child.
  Parameters:
  - org-id : a map with :root and :extension
  - rel-type-or-types - ODS relationship code (e.g. 'RE4' or collection #{\"RE4\"})
  See https://www.odsdatasearchandexport.nhs.uk/referenceDataCatalogue/Relationships_571324965.html"
  [org-id rel-type-or-types]
  (let [s (org-id->str org-id)]
    (cond
      (string? rel-type-or-types)
      (TermQuery. (Term. ^String rel-type-or-types s))
      :else
      (q-or (map #(TermQuery. (Term. ^String % s)) rel-type-or-types)))))


(def all-relationship-types
  #{"RE2" "RE3" "RE4" "RE5" "RE6" "RE8" "RE9" "RE10" "RE11"})

(def all-child-relation-types
  "A set of relationship types that conceptually mean that the target is the
  'parent'. RE2 is 'subdivision', RE3 is 'directed by', RE4 is 'commissioned by'
  and RE6 is 'operated by'. Other relation types are about geography, or
  describe partnerships, or relate to payee/payer relations."
  #{"RE2" "RE3" "RE4" "RE6"})

(defn child-relationships
  "Return a sequence of organisations that have the specified org as the target;
  in essence returning 'children' of the parent by virtue of one of the
  relationship types specified, or any 'child' relationship if omitted. For
  convenience, a role or sequence of roles may be provided to limit results
  to organisations with that or those roles.
  - rel-types - a string or collection of strings of relationship types (e.g. \"RE6\"
  - limit     - limit results, default 1000
  If rel-types is nil, a default set of relationship types conceptually fitting
  the idea of 'child' will be used."
  [^IndexSearcher searcher
   {:keys [org orgId root extension rel-types limit roles]}]
  (let [orgId (or orgId (if org (:orgId org) {:root (or root hl7-oid-health-and-social-care-organisation-identifier) :extension extension}))
        q (q-rel orgId (or rel-types all-child-relation-types))
        q' (if (seq roles) (q-and [q (q-roles roles)]) q)]
    (->> (do-query searcher q' (or limit 1000))
         (map doc->organisation))))

(defn make-search-query
  "Create a search query for an organisation.
  Parameters:
  - :s             : search for name or address of organisation
  - :n             : search for name of organisation
  - :address       : search within address of organisation
  - :fuzzy         : fuzziness factor (0-2)
  - :only-active?  : only include active organisations (default, true)
  - :roles         : a string or vector of roles
  - :from-location : a map containing:
       - :lat     : latitude (WGS84)
       - :lon     : longitude (WGS84)
       - :range   : range in metres
  - limit         : limit on number of search results"
  [{:keys [s n address fuzzy only-active? from-location roles _limit] :or {fuzzy 0 only-active? true}}]
  (let [{:keys [lat lon range]} from-location]
    (q-and (cond-> []
                   s
                   (conj (q-or [(q-name s fuzzy) (q-address s fuzzy)]))

                   n
                   (conj (q-name n fuzzy))

                   address
                   (conj (q-address address fuzzy))

                   roles
                   (conj (q-roles roles))

                   only-active?
                   (conj (q-active))

                   (and lat lon range)
                   (conj (q-location lat lon range))))))

(defn search
  "Search for an organisation.
  Parameters:
   - searcher : Lucene IndexSearcher
   - q        : a map containing your query terms.

  See `make-search-query` for the query terms.

  Warning: the results are lazily evaluated, so use `doall` if you plan
  on closing the IndexReader before processing the results."
  [^IndexSearcher searcher {:keys [_s _n _only-active? _roles from-location limit] :or {limit 1000} :as q}]
  (let [query (make-search-query q)
        {:keys [lat lon]} from-location
        result (if (and lat lon)
                 (do-raw-query searcher query limit (sort-by-distance lat lon))
                 (do-raw-query searcher query limit))]
    (map doc->organisation result)))

(comment
  (def api-key (str/trim-newline (slurp "/Users/mark/Dev/trud/api-key.txt")))
  api-key
  ;; download and build the index
  (require '[com.eldrix.clods.download :as dl])
  (def ods (dl/download {:api-key api-key :cache-dir "/tmp/trud" :batch-size 1000}))
  (keys ods)
  (:code-systems ods)

  ;; integrate NHS postcode directory
  (require '[com.eldrix.nhspd.core :as nhspd])
  (def nhspd (nhspd/open-index "/tmp/nhspd-2021-02"))
  (nhspd/fetch-postcode nhspd "CF14 4xw")
  (nhspd/fetch-wgs84 nhspd "CF14 4XW")

  (first (a/<!! (:organisations ods)))
  (build-index nhspd (:organisations ods) "/var/tmp/ods")

  ;; search for an organisation
  (def reader (open-index-reader "ods-2024-01-08.db"))
  (def searcher (IndexSearcher. reader))
  (read-metadata searcher "code-systems")
  (q-orgId "BE1EC")
  (fetch-org searcher "7A4BV")
  (child-relationships searcher {:org (fetch-org searcher nil "7A4")})
  (child-relationships searcher {:extension "7A4" :rel-types "RE6"})
  (do-raw-query searcher (q-orgId "RWMBV") 100)
  (time (filter :active (map doc->organisation (do-raw-query searcher (q-tokens "name" "rookwood hosp") 100))))
  (LatLonPoint/newDistanceQuery "latlon" 51.506764 3.1893604 1000)
  (map doc->organisation (do-raw-query searcher (LatLonPoint/newDistanceQuery "latlon" 52.71050609941029 -5.268334343112894 (* 20 1000)) 10))
  (LatLonDocValuesField/newDistanceSort "latlon" 52.71050609941029 -5.268334343112894)
  (map doc->organisation
       (do-raw-query searcher
                     (LatLonPoint/newDistanceQuery "latlon" 52.71050609941029 -5.268334343112894 (* 20 1000))
                     5
                     (Sort. (LatLonDocValuesField/newDistanceSort "latlon" 52.71050609941029 -5.268334343112894))))

  ; 51.506764 , -3.1893604
  (nhspd/fetch-wgs84 nhspd "CF14 4XW")
  (def monmouth (nhspd/fetch-wgs84 nhspd "np253eq"))
  monmouth
  (def cf144xw (nhspd/fetch-wgs84 nhspd "CF14 4XW"))

  (map doc->organisation (do-raw-query searcher (q-and [(q-roles "RO72") (q-location monmouth 10000)]) 10 (sort-by-distance monmouth)))
  (map doc->organisation (do-raw-query searcher (q-and [(q-roles "RO72") (q-location cf144xw 10000)]) 2 (sort-by-distance cf144xw)))


  (let [[lat lon] (nhspd/fetch-wgs84 nhspd "np25 3mm")]
    (search searcher {:s "caslte gate" :fuzzy 2 :from-location {:lat lat :lon lon} :roles "RO72"}))

  (take 1 (all-organizations reader))
  (search searcher {:address "MONMOUTH" :roles "RO177"})
  (search searcher {:s "castle gate" :roles "RO177"})
  (map :name (search searcher {:s "prince" :roles ["RO150" "RO198" "RO149" "RO108"]})))



