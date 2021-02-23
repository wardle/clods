(ns com.eldrix.clods.index
  "An NHS Organisation Directory Service (ODS) file-based index providing 
   UK organisational search functionality."
  (:require [clojure.core.async :as a]
            [clojure.string :as str]
            [taoensso.nippy :as nippy])
  (:import (org.apache.lucene.index Term IndexWriter IndexWriterConfig DirectoryReader IndexWriterConfig$OpenMode IndexReader)
           (org.apache.lucene.store FSDirectory)
           (org.apache.lucene.document Document Field$Store StoredField StringField LatLonPoint)
           (org.apache.lucene.search IndexSearcher TermQuery TopDocs ScoreDoc)
           (org.apache.lucene.analysis.standard StandardAnalyzer)
           (java.nio.file Paths)))

(set! *warn-on-reflection* true)

(defn make-organisation-doc
  "Turn an organisation into a Lucene document."
  [org]
  (let [doc (doto (Document.)
              (.add (StringField. "name" ^String (:name org) Field$Store/NO))
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
      (when (:active role) (.add doc (StringField. "role" (:id role) Field$Store/NO))))
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
    (a/<!!                                                ;; block until pipeline complete
     (a/pipeline                                          ;; pipeline for side-effects
      (.availableProcessors (Runtime/getRuntime))         ;; parallelism factor
      (doto (a/chan) (a/close!))                          ;; output channel - /dev/null
      (map (partial write-batch! writer))
      ch))
    (.forceMerge writer 1)))

(defn ^IndexReader open-index-reader
  [filename]
  (let [directory (FSDirectory/open (Paths/get filename (into-array String [])))]
    (DirectoryReader/open directory)))

(comment
  (def api-key (str/trim-newline (slurp "/Users/mark/Dev/trud/api-key.txt")))
  api-key
  ;; download and build the index
  (require '[com.eldrix.clods.download :as dl])
  (def ods (dl/download {:api-key api-key :cache-dir "/tmp/trud" :batch-size 1000}))
  (build-index (:organisations ods) "/var/tmp/ods")
  ;; search for an organisation
  
  )