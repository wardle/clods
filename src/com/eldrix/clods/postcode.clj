(ns com.eldrix.clods.postcode
  (:require
    [clojure.string :as s]
    [clojure.java.io :as io]
    [clojure.data.json :as json]
    [clojure.data.csv :as csv]
    [clojure.core.async :as async])

  (:import
    (java.io InputStreamReader)))

;; The NHS Postcode directory ("NHSPD") lists all current and terminated postcodes in the UK
;; and relates them to a range of current statutory administrative, electoral, health and other
;; geographies.
;;
;; Unfortunately, it is not possible to automatically download these data from a machine-readable
;; canonical resource, but the download is available manually.
;;
;; The February 2020 release is available at:
;; https://geoportal.statistics.gov.uk/datasets/nhs-postcode-directory-uk-full-february-2020
;;
;;

(def field-names ["PCD2" "PCDS" "DOINTR" "DOTERM" "OSEAST100M"
                  "OSNRTH100M" "OSCTY" "ODSLAUA" "OSLAUA" "OSWARD"
                  "USERTYPE" "OSGRDIND" "CTRY" "OSHLTHAU" "RGN"
                  "OLDHA" "NHSER" "CCG" "PSED" "CENED"
                  "EDIND" "WARD98" "OA01" "NHSRLO" "HRO"
                  "LSOA01" "UR01IND" "MSOA01" "CANNET" "SCN"
                  "OSHAPREV" "OLDPCT" "OLDHRO" "PCON" "CANREG"
                  "PCT" "OSEAST1M" "OSNRTH1M" "OA11" "LSOA11"
                  "MSOA11" "CALNCV" "STP"])

(defn normalize
  "Normalizes a postcode into uppercase 8-characters with left-aligned outward code and right-aligned inward code
  returning the original if normalization not possible"
  [pc]
  (s/upper-case (let [codes (s/split pc #"\s+")] (if (= 2 (count codes)) (apply #(format "%-5s %3s" %1 %2) codes) pc))))

(defn egif
  "Normalizes a postcode into uppercase with outward code and inward codes separated by a single space"
  [pc]
  (s/upper-case (s/replace pc #"\s+" " ")))


(defn import-postcodes
  "Import batches of postcodes to the function specified, each formatted as a vector representing
  [PCDS PCD2 json-data]."
  [in f]
  (with-open [is (io/input-stream in)]
    (->> is
         (InputStreamReader.)
         (csv/read-csv)
         (map #(zipmap field-names %))
         (map #(vector (get % "PCDS") (get % "PCD2") (json/write-str %)))
         (partition-all 10000)
         (run! #(f %)))))

(comment

  ;; this is the Feb 2020 release file (928mb)
  (def filename "/Users/mark/Downloads/NHSPD_FEB_2020_UK_FULL/Data/nhg20feb.csv")

  )
