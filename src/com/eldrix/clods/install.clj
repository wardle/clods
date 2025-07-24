(ns com.eldrix.clods.install
  (:require [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [com.eldrix.clods.core :as clods]
            [com.eldrix.nhspd.api :as nhspd]))


(def cli-options
  [[nil "--nhspd PATH" "Path to NHSPD index"]
   [nil "--api-key PATH" "Path to file containing API KEY"]
   [nil "--cache-dir PATH" "Path to cache directory for TRUD downloads"]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Usage: clj -M:install [options] <path>"
        ""
        "Options:"
        options-summary]
       (str/join \newline)))

(defn exit
  [status msg]
  (println msg)
  (System/exit status))

(defn install [{:keys [nhspd api-key cache-dir] :as opts} path]
  (cond
    (not path)
    (exit 1 "Missing path. You must specify index directory.")
    (not api-key)
    (exit 1 "Missing api-key. You must provide a path to a file containing TRUD api key")
    (not nhspd)
    (exit 1 "Missing NHSPD index directory.")
    :else
    (let [trud-api-key (str/trim-newline (slurp api-key))
          nhspd-svc (when nhspd (nhspd/open nhspd))]
      (clods/install path nhspd-svc trud-api-key (or cache-dir "/tmp/trud")))))

(defn -main [& args]
  (let [{:keys [options arguments summary errors]} (cli/parse-opts args cli-options)]
    (cond
      ;; asking for help?
      (:help options)
      (println (usage summary))
      ;; if we have any errors, exit with error message(s)
      errors
      (exit 1 (str/join \newline errors))
      ;; if we have no command, exit with error message
      (not= 1 (count arguments))
      (exit 1 (str "invalid parameters\n" (usage summary)))
      ;; invoke command
      :else (install options (first arguments)))))
