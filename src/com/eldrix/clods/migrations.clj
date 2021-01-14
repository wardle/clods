(ns com.eldrix.clods.migrations
  (:require [migratus.core :as migratus]))

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
(defn init
  "Initialize database schema.
  A database spec map can be provided in place of the default, or
  provide {:connection conn}  to use an already initialised java.sql.Connection
  or java.sql.DataSource."
  ([]
   (migratus/init config))
  ([db]
   (migratus/init (assoc config :db db))))

(defn migrate
  "Migrate database schema.
  A database spec map can be provided in place of the default, or
  provide {:connection conn}  to use an already initialised java.sql.Connection
  or java.sql.DataSource."
  ([]
   (migratus/migrate config))
  ([db]
   (migratus/migrate (assoc config :db db))))

(comment
  ;initialize the database using the 'init.sql' script
  (migratus/init config)

  (migratus/create config "add-grid-ref-indexes")

  ;list pending migrations
  (migratus/pending-list config)

  ;apply pending migrations
  (migratus/migrate config)

  (migratus/rollback config)
  )