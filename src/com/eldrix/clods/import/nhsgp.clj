(ns com.eldrix.clods.import.nhsgp
  "Functionality to import supplementary NHS organisational data not in the
  standard ODS XML distribution.

  Some of this information is available via TRUD subpack 58 - but not all.
  That release includes only current general practitioners, but we want both
  current and archived general practitioners. Therefore, we derive these data
  from a download direct from NHS Digital."
  (:require [clj-http.client :as http]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.core.async :as async])
  (:import (java.io File InputStreamReader)
           (java.util.zip ZipFile)
           (java.io InputStreamReader)))

(def n27-field-format
  "The standard ODS 27-field format headings"
  [:organisationCode
   :name
   :nationalGrouping
   :highLevelHealthGeography
   :address1
   :address2
   :address3
   :address4
   :address5
   :postcode
   :openDate
   :closeDate
   :statusCode
   :subtype
   :parent
   :joinParentDate
   :leftParentDate
   :telephone
   :nil
   :nil
   :nil
   :amendedRecord
   :nil
   :currentOrg
   :nil
   :nil
   :nil])

(def ^:private files
  "A list of general practitioner ODS files and their download locations for data not in the master XML file."
  {:egpcur {:name        "egpcur"
            :description "General practitioners"
            :url         "https://files.digital.nhs.uk/assets/ods/current/egpcur.zip"}
   :egparc {:name        "egparc"
            :description "Archived GP Practitioners"
            :url         "https://files.digital.nhs.uk/assets/ods/current/egparc.zip"}})

(defn- download [url target]
  (let [request (http/get url {:as :stream})
        buffer-size (* 1024 10)]
    (with-open [input (:body request)
                output (io/output-stream target)]
      (let [buffer (make-array Byte/TYPE buffer-size)]
        (loop []
          (let [size (.read input buffer)]
            (when (pos? size)
              (.write output buffer 0 size)
              (recur))))))))

(defn- file-from-zip
  "Reads from the zipfile specified, extracts the file `filename` and passes each line to your function `f`"
  [zipfile filename f]
  (with-open [zipfile (new ZipFile zipfile)]
    (when-let [entry (.getEntry zipfile filename)]
      (let [reader (InputStreamReader. (.getInputStream zipfile entry))]
        (run! f (csv/read-csv reader))))))

(defn- download-ods-file
  "Blocking; downloads the specified ODS filetype `t` (e.g. :egpcur) returning each item on the channel specified."
  ([t ch] (download-ods-file t ch true))
  ([t ch close?]
   (let [filetype (t files)
         temp (File/createTempFile (:name filetype) ".zip")]
     (when-not filetype
       (throw (IllegalArgumentException. (str "unsupported filetype: " t))))
     (download (:url filetype) temp)
     (file-from-zip temp (str (:name filetype) ".csv")
                    (fn [line]
                      (async/>!! ch (zipmap n27-field-format line))))
     (when close? (async/close! ch))
     (.delete temp))))

(defn stream-general-practitioners
  "Downloads data on current and archived general practitioners,
  returning data in batches on the returned core.async channel."
  [batch-size]
  (let [current (async/chan batch-size (partition-all batch-size))
        archive (async/chan batch-size (partition-all batch-size))]
    (async/thread (download-ods-file :egpcur current))
    (async/thread (download-ods-file :egparc archive))
    (async/merge [current archive])))

(comment
  (def gps (stream-general-practitioners 5))
  (async/<!! gps)

  )