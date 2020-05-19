(ns com.eldrix.clods.postcode
  (:require
    [clojure.string :as s]))


;; The NHS Postcode directory ("NHSPD") lists all current and terminated postcodes in the UK
;; and relates them to a range of current statutory administrative, electoral, health and other
;; geographies.
;;
;; Unfortunately, it is not possible to automatically download these data from a machine-readable
;; canonical resource, but the download is available manually.
;;
;; The November 2019 release is available at:
;; https://geoportal.statistics.gov.uk/datasets/nhs-postcode-directory-uk-full-november-2019
;; metadata:
;; https://www.arcgis.com/sharing/rest/content/items/d7b33b66949b4bc9b9065de7544ae4d1/info/metadata/metadata.xml?format=default&output=html
;;

(defn normalize
  "Normalizes a postcode into uppercase 8-characters with left-aligned outward code and right-aligned inward code
  returning the original if normalization not possible"
  [pc]
  (let [codes (s/split pc #"\s+" )] (if (= 2 (count codes)) (apply #(format "%-5s %3s" %1 %2) codes) pc)))

(defn egif
  "Normalizes a postcode into uppercase with outward code and inward codes separated by a single space"
  [pc]
  (s/replace pc #"\s+" " "))

