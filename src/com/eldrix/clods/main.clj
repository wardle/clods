(ns com.eldrix.clods.main
  (:require [clj-time.core :as t]
            [com.eldrix.clods.import :as import]
            [clj-time.format :as f]))

(defn time-str
  "Returns a string representation of a datetime in the local time zone."
  [dt]
  (f/unparse
    (f/with-zone (f/formatter "hh:mm aa") (t/default-time-zone))
    dt))

(defn -main []
  (println "Hello world, the time is" (time-str (t/now)))
  (print (import/find-by-code "/Users/mark/Downloads/hscorgrefdataxml_data_4.0.0_20200430000001/HSCOrgRefData_Full_20200427.xml" "W93036"))
  )