(ns com.eldrix.clods.import.nhspd
  "Provides functionality to import 'NHS Postcode Data'."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.core.async :as async]
            [clojure.string :as str])
  (:import (java.io InputStreamReader)))

(def nhspd-field-names
  ["PCD2" "PCDS" "DOINTR" "DOTERM" "OSEAST100M"
   "OSNRTH100M" "OSCTY" "ODSLAUA" "OSLAUA" "OSWARD"
   "USERTYPE" "OSGRDIND" "CTRY" "OSHLTHAU" "RGN"
   "OLDHA" "NHSER" "CCG" "PSED" "CENED"
   "EDIND" "WARD98" "OA01" "NHSRLO" "HRO"
   "LSOA01" "UR01IND" "MSOA01" "CANNET" "SCN"
   "OSHAPREV" "OLDPCT" "OLDHRO" "PCON" "CANREG"
   "PCT" "OSEAST1M" "OSNRTH1M" "OA11" "LSOA11"
   "MSOA11" "CALNCV" "STP"])


(defn import-postcodes
  "Import postcodes to the channel specified.
  Each item formatted as a vector of the format [PCDS PCD2 json-data].
  Parameters:
    - in     : An argument that can be coerced into an input stream (see io/input-stream)
    - ch     : The channel to use
    - close? : If the channel should be closed when done."
  ([in ch] (import-postcodes in ch true))
  ([in ch close?]
   (with-open [is (io/input-stream in)]
     (->> is
          (InputStreamReader.)
          (csv/read-csv)
          (map #(zipmap nhspd-field-names %))
          (map #(update % "OSNRTH1M" (fn [coord] (when-not (str/blank? coord) (Integer/parseInt coord)))))
          (map #(update % "OSEAST1M" (fn [coord] (when-not (str/blank? coord) (Integer/parseInt coord)))))
          (map #(vector (get % "PCDS") (get % "PCD2") (json/write-str %)))
          (run! #(async/>!! ch %))))
   (when close? (async/close! ch))))

(defn get-latest-release
  "Returns the latest NHSPD release.
   At the moment, there is no available API for this, so we fake by using a
   local embedded datafile that can be updated with the latest release date
  and URL. The NHSPD is currently released quarterly."
  []
  (let [releases (edn/read-string (slurp (io/resource "nhspd.edn")))]
    (first (reverse (sort-by :release releases)))))

(comment
  ;; this is the Feb 2020 release file (928mb)
  (def nhspd "/Users/mark/Downloads/NHSPD_FEB_2020_UK_FULL/Data/nhg20feb.csv")
  (def ch (async/chan))
  (async/thread (import-postcodes nhspd ch))
  (async/<!! ch)

  (def release (get-latest-release))
  (require '[com.eldrix.trud.cache :as trud])
  (def temp-file (java.nio.file.Files/createTempFile "nhspd" ".zip" (make-array java.nio.file.attribute.FileAttribute 0)))
  (:url release)
  (#'trud/download-url (:url release) temp-file)
  temp-file


  )