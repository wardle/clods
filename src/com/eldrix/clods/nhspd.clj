(ns com.eldrix.clods.nhspd
  (:require [com.eldrix.nhspd.core :as nhspd]))


(defn -main [& args]
  (if-not (= 1 (count args))
    (println "Missing directory. Usage: clj -M:nhspd <dir> where dir is index directory (e.g. /var/nhspd)")
    (nhspd/write-index (first args))))
