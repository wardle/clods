(ns com.eldrix.clods.cli
  (:require [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [com.eldrix.clods.core :as clods]
            [com.eldrix.nhspd.core :as nhspd]))


(def cli-options
  [[nil "--nhspd PATH" "Path to NHSPD index (optional; will be downloaded)"]
   [nil "--api-key PATH" "Path to file containing API KEY"]
   [nil "--cache-dir PATH" "Path to cache directory for TRUD downloads"]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Usage: clods [options] command [parameters]"
        ""
        "Options:"
        options-summary
        ""
        "Commands:"
        " download [path] Create or update an index at path specified."]
       (str/join \newline)))

(defn exit
  [status msg]
  (println msg)
  (System/exit status))

(defn download [{:keys [nhspd api-key cache-dir] :as opts} [path & more]]
  (cond
    (not path)
    (exit 1 "Missing path. You must specify index directory.")
    more
    (exit 1 "You must specify only a single path for the index")
    (not api-key)
    (exit 1 "Missing api-key. You must provide a path to a file containing TRUD api key")
    :else
    (let [trud-api-key (str/trim-newline (slurp api-key))
          nhspd-svc (when nhspd (nhspd/open-index nhspd))]
      (clods/install path trud-api-key (or cache-dir "/tmp/trud") nhspd-svc))))

(def commands
  {"download" {:fn download}})

(defn invoke-command [cmd opts args]
  (if-let [f (:fn cmd)]
    (f opts args)
    (exit 1 "error: not implemented")))

(defn -main [& args]
  (let [{:keys [options arguments summary errors]} (cli/parse-opts args cli-options)
        command (get commands ((fnil str/lower-case "") (first arguments)))]
    (cond
      ;; asking for help?
      (:help options)
      (println (usage summary))
      ;; if we have any errors, exit with error message(s)
      errors
      (exit 1 (str/join \newline errors))
      ;; if we have no command, exit with error message
      (not command)
      (exit 1 (str "invalid command\n" (usage summary)))
      ;; invoke command
      :else (invoke-command command options (rest arguments)))))
