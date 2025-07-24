(ns com.eldrix.clods.nhspd
  (:require [com.eldrix.nhspd.api :as nhspd]))


(defn -main [& args]
  (if-not (= 1 (count args))
    (println "Missing directory. Usage: clj -M:nhspd <file> where <file> is index file (e.g. /var/nhspd.db)")
    (nhspd/create-latest (first args))))
