(ns com.eldrix.clods.db
  (:require [clojure.data.json :as json]
            [clojure.tools.logging.readable :as log]
            [migratus.core :as migratus]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection])
  (:import (java.sql Connection)
           (com.zaxxer.hikari HikariDataSource)))


(set! *warn-on-reflection* true)

(def datasource (atom nil))

(defn connection-pool-start [config]
  (let [ds (connection/->pool HikariDataSource config)]
    (reset! datasource ds)
    @datasource))

(defn connection-pool-stop []
  (.close @datasource))

(defn single-connection-start [config]
  (let [ds (next.jdbc/get-datasource config)]
    (reset! datasource ds)))

(defn single-connection-stop []
  (.close @datasource))

(def config {:store                :database
             :migration-dir        "migrations/"
             :init-script          "init.sql"               ;script should be located in the :migration-dir path
             ;defaults to true, some databases do not support
             ;schema initialization in a transaction
             :init-in-transaction? false
             :migration-table-name "_migrations"
             :db                   {:classname   "org.postgresql.Driver"
                                    :subprotocol "postgresql"
                                    :subname     "ods"}})

(defn migrate []
  "Migrate database schema."
  (migratus/migrate config))

(defn insert-code-systems
  "Inserts code systems into the database 'ds' specified."
  ;; codesystems = (ods/codesystems in)
  [^Connection conn codesystems]
  (let [v (map #(vector (:oid %) (:name %)) codesystems)]
    (with-open [ps (jdbc/prepare conn ["insert into codesystems (oid,name) values (?,?) on conflict (oid) do update set name = EXCLUDED.name"])]
      (next.jdbc.prepare/execute-batch! ps v))))

(defn insert-codes
  "Import the codes into the database 'db' specified."
  ;;codes (ods/all-codes in)
  [^Connection conn codes]
  (let [v (map #(vector (:id %) (:displayName %) (:codeSystem %)) codes)]
    (with-open [ps (jdbc/prepare conn ["insert into codes (id,display_name,code_system) values (?,?,?) on conflict (id) do update set display_name = EXCLUDED.display_name, code_system = EXCLUDED.code_system"])]
      (next.jdbc.prepare/execute-batch! ps v))))

(defn insert-organisations
  "Import a batch of organisations."
  [^Connection conn orgs]
  (with-open [ps (jdbc/prepare conn ["insert into organisations (id,name,active,data) values (?,?,?,?::jsonb) on conflict (id) do update set name = EXCLUDED.name, active= EXCLUDED.active, data = EXCLUDED.data"])]
    (let [v (map #(vector
                    (str (get-in % [:orgId :root]) "|" (get-in % [:orgId :extension]))
                    (:name %)
                    (:active %)
                    (json/write-str %)) orgs)]
      (next.jdbc.prepare/execute-batch! ps v))))

(def general-practitioner-org-oid
  "We can safely prepend this oid to organisations referenced in the 27 field format
file to generate a globally unique reference"
  "2.16.840.1.113883.2.1.3.2.4.18.48")

(defn insert-general-practitioners
  "Inserts a batch of general practitioners in the database.
  We have a special problem with the GP files, in that they can be registered to an organisation that doesn't exist
  even in the ODS archive. We handle that by catching and ignoring foreign key constraints, and therefore
  don't import GPs without a valid organisation."
  [^Connection conn gps]
  (with-open [ps (jdbc/prepare conn ["insert into general_practitioners (id, name, organisation, data) values (?,?,?,?::jsonb) on conflict (id) do update set name = EXCLUDED.name, organisation = EXCLUDED.organisation, data = EXCLUDED.data"])]
    (doseq [line gps]
      (try
        (next.jdbc.prepare/set-parameters ps [(:organisationCode line) (:name line) (str general-practitioner-org-oid "|" (:parent line)) (json/write-str line)])
        (jdbc/execute! ps)
        (catch Exception e (when-not (:leftParentDate line) (log/error e "failed to import: " line)))))))

(defn insert-postcodes
  "Import/update postcode data (NHSPD e.g. nhg20feb.csv) to the datasource `ds` specified."
  [^Connection conn postcodes]
  (with-open [ps (jdbc/prepare conn ["insert into postcodes (PCD2,PCDS,DATA) values (?,?,?::jsonb) on conflict (PCD2) do update set PCDS = EXCLUDED.PCDS, DATA=EXCLUDED.DATA"])]
    (next.jdbc.prepare/execute-batch! ps postcodes)))

(comment
  ;initialize the database using the 'init.sql' script
  (migratus/init config)

  ;list pending migrations
  (migratus/pending-list config)

  ;apply pending migrations
  (migratus/migrate config)

  )