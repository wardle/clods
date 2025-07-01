(ns src.com.eldrix.clods.core-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is use-fixtures]]
   [com.eldrix.clods.core :as clods]
   [com.eldrix.nhspd.core :as nhspd]
   [clojure.string :as str]))

(def ^:dynamic *svc* nil)

(defn trud-api-key
  "Returns the TRUD API key."
  []
  (str/trim-newline (slurp (or (System/getenv "TRUD_API_KEY_FILE") "api-key.txt"))))

(defn live-test-fixture [f]
  (if (and (.exists (io/file "latest-clods.db")) (.exists (io/file "nhspd")))
    (with-open [clods (clods/open-index {:f "latest-clods.db" :nhspd-dir "nhspd"})]
      (println "WARNING: skipping test of install-release as using existing latest-clods.db. Delete this file if required.")
      (binding [*svc* clods]
        (f)))
    (let [api-key (trud-api-key)]
      (println "Installing NHSPD and clods releases")
      (nhspd/write-index "nhspd")
      (with-open [nhspd (nhspd/open-index "nhspd")]
        (clods/install "latest-clods.db" nhspd api-key "cache")
        (with-open [clods (clods/open-index {:f "latest-clods.db" :nhspd nhspd})]
          (binding [*svc* clods]
            (f)))))))

(use-fixtures :once live-test-fixture)

(deftest fetch-org
  (let [cavuhb (clods/fetch-org *svc* nil "7A4")]
    (is (= "7A4" (get-in cavuhb [:orgId :extension])))
    (is (= "CARDIFF & VALE UNIVERSITY LHB" (:name cavuhb)))))

(deftest search-org
  (let [results (clods/search-org *svc* {:as :ext-orgs :s "Cardiff & Vale" :rc "RC1"})]
    (is (= (clods/fetch-org *svc* nil "7A4") (first results)))))




