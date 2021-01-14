(ns com.eldrix.clods.postcode
  (:require
    [clojure.string :as str]))

(defn normalize
  "Normalizes a postcode into uppercase 8-characters with left-aligned outward code and right-aligned inward code
  returning the original if normalization not possible"
  [pc]
  (str/upper-case (let [codes (str/split pc #"\s+")] (if (= 2 (count codes)) (apply #(format "%-5s %3s" %1 %2) codes) pc))))

(defn egif
  "Normalizes a postcode into uppercase with outward code and inward codes separated by a single space"
  [pc]
  (str/upper-case (str/replace pc #"\s+" " ")))

(defn distance-between
  "Calculates the distance between two postcodes, determined by the square root of the sum of the square of
  the difference in grid coordinates (Pythagoras), result in metres.
  Parameters:
  - pc1d - first postcode NHSPD data (map)
  - pc2d - second postcode NHSPD data (map)"
  [pcd1 pcd2]
  (let [n1 (:OSNRTH1M pcd1)
        n2 (:OSNRTH1M pcd2)
        e1 (:OSEAST1M pcd1)
        e2 (:OSEAST1M pcd2)]
    (when (every? number? [n1 n2 e1 e2])
      (let [nd (- n1 n2)
            ed (- e1 e2)]
        (Math/sqrt (+ (* nd nd) (* ed ed)))))))
