(ns com.eldrix.clods.updatelog
  (:require [next.jdbc :as jdbc]
            [next.jdbc.date-time]                           ;; needed for adding automatic handling of java.time.LocalDate -> database
            [next.jdbc.sql :as sql])
  (:import (java.time LocalDate)))

(next.jdbc.date-time/read-as-local)

(defn log-start-update
  "Logs the attempted start of an import of a new reference data release."
  [ds ^String namespace ^String identifier ^LocalDate release-date]
  (sql/insert! ds "updatelog" {:action       "start"
                               :namespace    namespace
                               :identifier   identifier
                               :release_date release-date}))
(defn log-end-update
  "Logs the completion of an import of a new reference data release."
  [ds ^String namespace ^String identifier ^LocalDate release-date]
  (sql/insert! ds "updatelog" {:action       "end"
                               :namespace    namespace
                               :identifier   identifier
                               :release_date release-date}))

(defn fetch-installed
  "Returns installed reference data releases."
  [conn]
  (->> (sql/query conn ["select namespace,identifier, max(release_date) as release_date from updatelog where action = ? group by namespace,identifier" "end"])
       (map #(vector (keyword (:updatelog/namespace %) (:updatelog/identifier %)) (:release_date %)))
       (into {})))

(comment
  (def ds (jdbc/get-datasource "jdbc:postgresql://localhost/ods"))
  (log-start-update ds "uk.nhs.trud" "item-294" (LocalDate/now))
  (log-end-update ds "uk.nhs.trud" "item-294" (LocalDate/now))
  
  (sql/query ds ["select namespace,identifier, max(release_date) as release_date from updatelog where action = ? group by namespace,identifier" "end"])
  (fetch-installed ds)
  )