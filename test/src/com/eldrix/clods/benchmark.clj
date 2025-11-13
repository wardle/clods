(ns com.eldrix.clods.benchmark
  "Performance benchmarks for organizational succession queries.

  Run with: clj -M:bench

  Benchmarks active-successors performance with and without batch mode,
  demonstrating the N+1 problem and validating index performance.
  Also benchmarks connection pooling benefits."
  (:require [criterium.core :as crit]
            [com.eldrix.clods.core :as clods]))

(def db-file "latest-clods.db")
(def nhspd-file "latest-nhspd.db")

(defn has-active-successor?
  "Check if an organization has at least one active successor."
  [ods org-code]
  (seq (clods/org-code->active-successors ods org-code {:as :codes})))

(defn find-test-orgs
  "Find N inactive organizations that have active successors.
  Returns a sequence of org codes.

  Options:
  - :size - sample size (default 2000)
  - :recurse - if true, recursively samples with larger size if insufficient matches found;
               if false, throws exception (default true)"
  ([ods n]
   (find-test-orgs ods n {}))
  ([ods n {:keys [size recurse] :or {size 2000 recurse true}}]
   (let [results (->> (clods/random-orgs ods size)
                      (remove :active)
                      (map #(get-in % [:orgId :extension]))
                      (filter #(has-active-successor? ods %))
                      (take n))]
     (if (< (count results) n)
       (if recurse
         (find-test-orgs ods n {:size (* size 2) :recurse false})
         (throw (ex-info "Insufficient test organizations found"
                         {:requested n :found (count results) :sample-size size})))
       results))))

(defn benchmark-single
  "Benchmark single organization query."
  []
  (println "\n=== Single Organization Query ===\n")
  (with-open [ods (clods/open-index {:f db-file :nhspd-file nhspd-file})]
    (when-let [org-code (first (find-test-orgs ods 1))]
      (println "Test org:" org-code)
      (println)
      (crit/quick-bench
       (clods/org-code->active-successors ods org-code {:as :codes})))))

(defn benchmark-batch
  "Benchmark batch queries, comparing N+1 vs batch approach."
  []
  (println "\n=== Batch Query Comparison ===\n")
  (with-open [ods (clods/open-index {:f db-file :nhspd-file nhspd-file})]
    (let [org-codes (find-test-orgs ods 20)]
      (println "Found" (count org-codes) "test organizations\n")

      (println "N+1 approach:")
      (crit/quick-bench
       (doall (map #(clods/org-code->active-successors ods % {:as :codes}) org-codes)))

      (println "\nBatch approach:")
      (crit/quick-bench
       (clods/org-codes->active-successors ods org-codes {:as :codes})))))

(defn benchmark-fetch
  "Benchmark fetching organization data for single vs multiple organizations."
  []
  (println "\n=== Fetch Organization Benchmarks ===\n")
  (with-open [ods (clods/open-index {:f db-file :nhspd-file nhspd-file})]
    (let [org-codes (take 20 (map #(get-in % [:orgId :extension])
                                  (clods/random-orgs ods 100)))]
      (println "Testing with" (count org-codes) "random organizations\n")

      (println "Single organization fetch:")
      (crit/quick-bench
       (clods/fetch-org ods (first org-codes)))

      (println "\nN+1 approach (map fetch-org over 20 orgs):")
      (crit/quick-bench
       (doall (map #(clods/fetch-org ods %) org-codes)))

      (println "\nBatch approach (fetch-orgs):")
      (crit/quick-bench
       (doall (clods/fetch-orgs ods org-codes))))))

(defn benchmark-pooling
  "Benchmark connection pooling benefits for N+1 queries.

  Tests whether using a connection pool helps with N+1 problems when
  making many sequential database queries (like fetching multiple organizations
  one at a time vs using batch functions)."
  []
  (println "\n=== Connection Pooling Benchmarks ===\n")
  (let [org-codes (with-open [ods (clods/open-index {:f db-file :nhspd-file nhspd-file})]
                    (take 50 (map #(get-in % [:orgId :extension])
                                  (clods/random-orgs ods 100))))]
    (println "Testing with" (count org-codes) "random organizations\n")

    (println "Without pooling - N+1 fetch:")
    (with-open [ods (clods/open-index {:f db-file :nhspd-file nhspd-file})]
      (crit/quick-bench
       (doall (map #(clods/fetch-org ods %) org-codes))))

    (println "\nWith pooling (max 10 connections) - N+1 fetch:")
    (with-open [ods (clods/open-index {:f db-file :nhspd-file nhspd-file :pool {:maximumPoolSize 10}})]
      (crit/quick-bench
       (doall (map #(clods/fetch-org ods %) org-codes))))

    (println "\nWith pooling (max 5 connections) - N+1 fetch:")
    (with-open [ods (clods/open-index {:f db-file :nhspd-file nhspd-file :pool {:maximumPoolSize 5}})]
      (crit/quick-bench
       (doall (map #(clods/fetch-org ods %) org-codes))))

    (println "\nBatch approach (baseline - should be fastest):")
    (with-open [ods (clods/open-index {:f db-file :nhspd-file nhspd-file})]
      (crit/quick-bench
       (doall (clods/fetch-orgs ods org-codes))))))

(defn -main [& _args]
  (println "=== Organizational Succession Query Benchmarks ===")
  (benchmark-single)
  (benchmark-batch)
  (benchmark-fetch)
  (benchmark-pooling)
  (println "\n=== Benchmarks Complete ==="))
