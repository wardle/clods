(ns com.eldrix.clods.core
  (:require [clojure.tools.logging.readable :as log]
            [com.eldrix.clods.download :as dl]
            [com.eldrix.clods.index :as index]
            [com.eldrix.nhspd.core :as nhspd])
  (:import (org.apache.lucene.search IndexSearcher)
           (java.io Closeable)
           (com.eldrix.nhspd.core NHSPD)
           (java.nio.file.attribute FileAttribute)
           (java.nio.file Files)))

(defn download
  "Download the latest ODS release.
  Returns a map containing data including codesystems, codes and the
  organisations.
  See `com.eldrix.clods.download/download`."
  [params]
  (dl/download params))

(defn install
  "Download and install the latest release using the defined NHSPD service.
  Parameters:
  - dir       : directory in which to build ODS service
  - nhspd     : an NHS postcode directory service
  - api-key   : TRUD api key
  - cache-dir : TRUD cache directory."
  [^String dir ^NHSPD nhspd api-key cache-dir ]
  (log/info "Installing NHS organisational data index to:" dir)
  (let [ods (download {:api-key api-key :cache-dir cache-dir})]
    (index/build-index nhspd (:organisations ods) dir))
  (log/info "Finished creating index at " dir))

(defn merge-coords-from-postcode
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

(defn search
  "Search for an organisation
  Parameters :
  - searcher : A Lucene IndexSearcher
  - nhspd    : NHS postcode directory service
  - params   : Search parameters; a map containing:
    |- :s             : search for name of organisation
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
  (fetch-postcode [this pc] "Return NHSPD data about the specified postcode.")
  (fetch-wgs84 [this pc] "Returns WGS84 lat/long coordinates about the postcode."))

(defn open-index
  [ods-dir nhspd-dir]
  (let [reader (index/open-index-reader ods-dir)
        searcher (IndexSearcher. reader)
        nhspd (nhspd/open-index nhspd-dir)]
    (reify
      ODS
      (fetch-org [_ root extension] (index/fetch-org searcher root extension))
      (search-org [_ params] (search searcher nhspd params))
      (fetch-postcode [_ pc] (nhspd/fetch-postcode nhspd pc))
      (fetch-wgs84 [_ pc] (nhspd/fetch-wgs84 nhspd pc))
      Closeable
      (close [_]
        (.close reader)
        (.close nhspd)))))

(comment

  (with-open [idx (open-index "/var/tmp/ods" "/tmp/nhspd-2021-02")]
    (fetch-org idx nil "RWMBV"))

  (with-open [idx (open-index "/var/tmp/ods" "/tmp/nhspd-2021-02")]
    (doall (search-org idx {:s "vale" :limit 1})))

  (with-open [idx (open-index "/var/tmp/ods" "/tmp/nhspd-2021-02")]
    (doall (search-org idx {:s "vale" :limit 2 :from-location {:postcode "CF14 4XW"}})))

  ;; find surgeries within 2k of Llandaff North, in Cardiff
  (with-open [idx (open-index "/var/tmp/ods" "/tmp/nhspd-2021-02")]
    (doall (search-org idx {:roles ["RO177" "RO72"] :from-location {:postcode "CF14 2HD" :range 5000}})))
  )

