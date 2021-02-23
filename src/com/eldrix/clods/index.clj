(ns com.eldrix.clods.index
  "An NHS Organisation Directory Service (ODS) file-based index providing 
   UK organisational search functionality."
  (:require [clojure.core.async :as a]
            [clojure.string :as str]
            [taoensso.nippy :as nippy])
  (:import (org.apache.lucene.index Term IndexWriter IndexWriterConfig DirectoryReader IndexWriterConfig$OpenMode IndexReader)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.document Document Field$Store StoredField TextField StringField LatLonPoint)
           (org.apache.lucene.search IndexSearcher TermQuery TopDocs ScoreDoc BooleanQuery$Builder BooleanClause$Occur Query)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (java.nio.file Paths)))

(set! *warn-on-reflection* true)

(def hl7-oid-health-and-social-care-organisation-identifier
  "The default organisation root is the HL7 OID representing a
  HealthAndSocialCareOrganisationIdentifier"
  "2.16.840.1.113883.2.1.3.2.4.18.48")

(defn make-organisation-doc
  "Turn an organisation into a Lucene document."
  [org]
  (let [doc (doto (Document.)
              (.add (StringField. "root" ^String (get-in org [:orgId :root]) Field$Store/NO))
              (.add (StringField. "extension" ^String (get-in org [:orgId :extension]) Field$Store/NO))
              (.add (TextField. "name" (:name org) Field$Store/YES))
              (.add (StringField. "active" (str (:active org)) Field$Store/NO))
              (.add (StringField. "address"
                                  (str/join " " [(get-in org [:location :address1])
                                                 (get-in org [:location :address2])
                                                 (get-in org [:location :county])
                                                 (get-in org [:location :postcode])
                                                 (get-in org [:location :country])])
                                  Field$Store/NO))
              (.add (StoredField. "data" ^bytes (nippy/freeze org))))]
    (doseq [role (:roles org)]
      (when (:active role) (.add doc (StringField. "role" ^String (:id role) Field$Store/NO))))
    doc))

(defn write-batch! [^IndexWriter writer orgs]
  (dorun (map #(.addDocument writer (make-organisation-doc %)) orgs))
  (.commit writer))

(defn ^IndexWriter open-index-writer
  [filename]
  (let [analyzer (StandardAnalyzer.)
        directory (FSDirectory/open (Paths/get filename (into-array String [])))
        writer-config (doto (IndexWriterConfig. analyzer)
                        (.setOpenMode IndexWriterConfig$OpenMode/CREATE_OR_APPEND))]
    (IndexWriter. directory writer-config)))

(defn build-index
  "Build an index from NHS ODS data streamed on the channel specified."
  [ch out]
  (with-open [writer (open-index-writer out)]
    (a/<!!                                                  ;; block until pipeline complete
      (a/pipeline                                           ;; pipeline for side-effects
        (.availableProcessors (Runtime/getRuntime))         ;; parallelism factor
        (doto (a/chan) (a/close!))                          ;; output channel - /dev/null
        (map (partial write-batch! writer))
        ch))
    (.forceMerge writer 1)))

(defn ^IndexReader open-index-reader
  [filename]
  (let [directory (FSDirectory/open (Paths/get filename (into-array String [])))]
    (DirectoryReader/open directory)))


(defn ^Query q-orgId
  "Make a query for the identifier specified.
  - root      : (optional) root OID
  - extension : organisation extension (code)."
  ([^String extension] (q-orgId hl7-oid-health-and-social-care-organisation-identifier extension))
  ([^String root ^String extension]
  (-> (BooleanQuery$Builder.)
      (.add (TermQuery. (Term. "root" root)) BooleanClause$Occur/MUST)
      (.add (TermQuery. (Term. "extension" extension)) BooleanClause$Occur/MUST)
      (.build))))

(defn fetch-org
  "Returns NHS ODS data for the organisation specified.
  Parameters:
   - searcher  : Lucene IndexSearcher
   - root      : (optional) the identifier root;
                 default '2.16.840.1.113883.2.1.3.2.4.18.48'
   - extension : organisation code; eg. '7A4BV'."
  ([^IndexSearcher searcher extension] (fetch-org searcher hl7-oid-health-and-social-care-organisation-identifier extension))
  ([^IndexSearcher searcher root extension]
   (when-let [score-doc ^ScoreDoc (first (seq (.-scoreDocs ^TopDocs (.search searcher (q-orgId root extension) 1))))]
     (when-let [doc (.doc searcher (.-doc score-doc))]
       (nippy/thaw (.-bytes (.getBinaryValue doc "data")))))))

(defn make-search-query [q]
  )

(defn search
  "Search for an organisation.
  Parameters:
   - searcher : Lucene IndexSearcher
   - q        : a map containing your query terms."
  [^IndexSearcher searcher q]
  )

(comment
  (def api-key (str/trim-newline (slurp "/Users/mark/Dev/trud/api-key.txt")))
  api-key
  ;; download and build the index
  (require '[com.eldrix.clods.download :as dl])
  (def ods (dl/download {:api-key api-key :cache-dir "/tmp/trud" :batch-size 10000}))
  (first (a/<!! (:organisations ods)))
  (build-index (:organisations ods) "/var/tmp/ods")

  ;; search for an organisation
  (def reader (open-index-reader "/var/tmp/ods"))
  (def searcher (IndexSearcher. reader))
  (q-orgId "BE1EC")
  (fetch-org searcher "7A4BV"))