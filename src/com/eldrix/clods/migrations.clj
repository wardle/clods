(ns com.eldrix.clods.migrations
  (:require [migratus.core :as migratus]))

(def config {:store                :database
             :migration-dir        "migrations/"
             :init-script          "init.sql" ;script should be located in the :migration-dir path
             ;defaults to true, some databases do not support
             ;schema initialization in a transaction
             :init-in-transaction? false
             :migration-table-name "_migrations"
             :db {:classname   "org.postgresql.Driver"
                  :subprotocol "postgresql"
                  :subname     "ods"}})

(defn init []
  (migratus/init config))

(defn migrate []
  (migratus/migrate config))

(comment
  ;initialize the database using the 'init.sql' script
  (migratus/init config)

  ;apply pending migrations
  (migratus/migrate config)

  ;rollback the last migration applied
  (migratus/rollback config)

  ;bring up migrations matching the ids
  (migratus/up config 20111206154000)

  ;bring down migrations matching the ids
  (migratus/down config 20111206154000)

  )