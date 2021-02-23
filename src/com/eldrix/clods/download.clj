(ns com.eldrix.clods.download
  (:require [com.eldrix.clods.ods :as ods]
            [com.eldrix.trud.core :as trud]
            [com.eldrix.trud.zip :as ziputils]
            [clojure.tools.logging.readable :as log]
            [clojure.core.async :as a])
  (:import (java.time LocalDate)))


(defn- do-download
  "Downloads the latest ODS distribution file directly from UK TRUD.
  We can use the TRUD tooling to automatically download the release (341).
  If last-update is current, download will be skipped and `nil` returned.
  This file *should* contain two nested zip files:
   - archive.zip
   - fullfile.zip
  Returns a map with the following keys:
   - release   - data directly from the TRUD API
   - paths     - all downloaded paths
   - xml-files - sequence of `java.nio.file.Path`s to the uncompressed XML files."
  ([api-key cache-dir] (do-download api-key cache-dir nil))
  ([api-key cache-dir ^LocalDate last-update]
   (let [latest (trud/get-latest {:api-key api-key :cache-dir cache-dir} 341 last-update)]
     (if-not (:needsUpdate? latest)
       (log/info "skipping download ODS XML distribution files: already up-to-date.")
       (do (log/info "processing ODS XML distribution files." (select-keys latest [:itemIdentifier :releaseDate :archiveFilePath]))
           (let [all-files (ziputils/unzip2 [(:archiveFilePath latest)
                                             ["archive.zip" #"\w+.xml"]
                                             ["fullfile.zip" #"\w+.xml"]])]
             {:release   latest
              :paths     all-files
              :xml-files (concat (get-in all-files [1 1])
                                 (get-in all-files [2 1]))}))))))

(defn download
  "Download the latest NHS ODS distribution.
  Parameters:
  - :api-key     : NHS Digital 'TRUD' api-key
  - :cache-dir   : TRUD cache directory
  - :last-update : (optional) date of last update.
                   If provided, download skipped when no new release exists.
  - :nthreads    : number of threads to use; default num processors
  - :batch-size  : batch size for stream of organisations; default 100
  - :cleanup?    : add a shutdown hook to delete temporary files?

  Results:
  - A map containing the following keys:
    - :release       : the TRUD API release data
    - :manifests     : a sequence of manifest descriptions for each XML file
    - :code-systems  : code systems and codes (merged from all XML files)
    - :organisations : a `clojure.core.async` channel of all organisations
                       merging data from all XML files.
    - :paths         : a sequence of temporary file paths; delete when done.

  Code systems are keyed by a tuple of namespace and code. For example:
  [\"2.16.840.1.113883.2.1.3.2.4.17.507\" \"RO144\"]
   - '2.16.840.1.113883.2.1.3.2.4.17.507' : HL7 Organisation Role Type
   - 'RO144'                              : Welsh Local Health Board."
  [{:keys [api-key cache-dir ^LocalDate last-update cleanup? nthreads batch-size]
    :or   {nthreads (.availableProcessors (Runtime/getRuntime)) batch-size 100} :as opts}]
  (log/info "preparing to download and process latest ODS XML distribution" {:cache-dir cache-dir :nthreads nthreads :batch-size batch-size})
  (when-let [downloaded (do-download api-key cache-dir last-update)]
    (when (= 0 (count (:xml-files downloaded)))
      (throw (ex-info "no XML files identified in ODS XML release! Has structure changed?" {:paths (:paths downloaded)})))
    (when cleanup? (.addShutdownHook (Runtime/getRuntime)
                                     (Thread. (ziputils/delete-paths (:paths downloaded)))))
    (loop [xml-file-paths (:xml-files downloaded)           ; loop through and process each XML file
           result {}]
      (let [path (first xml-file-paths)]
        (if-not path
          (-> result
              (assoc :release (:release downloaded))
              (assoc :paths (:paths downloaded))
              (update :organisations a/merge))
          (let [f (.toFile path)
                mft (ods/manifest f)]
            (log/info "processing xml file: " {::manifest mft})
            (if-not (= (:version mft) ods/supported-ods-xml-version)
              (log/fatal "unsupported ODS XML version. expected" ods/supported-ods-xml-version "got:" (:version mft))
              (recur
                (rest xml-file-paths)
                {:manifests     (conj (:manifests result) mft)
                 :code-systems  (merge (:code-systems result) (let [codes (ods/all-codes f)] (zipmap (map #(vector (:codeSystem %) (:id %)) codes) codes)))
                 :organisations (conj (:organisations result) (ods/stream-organisations f nthreads batch-size))}))))))))

(comment
  ;; use the NHS TRUD service to download the files we need for NHS ODS XML  (distribution 341)
  (def api-key (clojure.string/trim-newline (slurp "/Users/mark/Dev/trud/api-key.txt")))
  api-key
  (def xml-files (do-download api-key "/tmp/trud"))
  (def ods (download {:api-key api-key :cache-dir "/tmp/trud" :cleanup? true}))
  (dissoc ods :code-systems :paths)
  (:code-systems ods)
  (count (:code-systems ods))
  (first (:code-systems ods))
  (a/<!! (:organisations ods))
  )