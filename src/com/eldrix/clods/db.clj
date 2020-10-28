(ns com.eldrix.clods.db
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [com.eldrix.clods.postcode :as postcode]
            [clojure.tools.logging.readable :as log]
            [migratus.core :as migratus]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection])
  (:import (java.sql Connection)
           (com.zaxxer.hikari HikariDataSource)))

(defonce ^javax.sql.DataSource datasource (atom nil))

(defn connection-pool-start [config]
  (let [ds (connection/->pool HikariDataSource config)]
    (reset! datasource ds)
    @datasource))

(defn connection-pool-stop []
  (.close @datasource))

(defn fetch-postcode [pcode]
  (when-let [pc (:pc (jdbc/execute-one! @datasource
                                        ["SELECT data::varchar as pc FROM postcodes where pcd2 = ?"
                                         (postcode/egif pcode)]))]
    (json/read-str pc :key-fn keyword)))

(defn fetch-org
  "Fetches an organisation by `root` and `identifier` from the data store"
  ([id] (fetch-org "2.16.840.1.113883.2.1.3.2.4.18.48" id))
  ([root id]
   (when-let [org (:org (jdbc/execute-one! @datasource
                                           ["SELECT data::varchar as org FROM organisations WHERE id = ?"
                                            (str root "|" id)]))]
     (json/read-str org :key-fn keyword))))

(defn name-query [s] (str "%" (str/replace (str/upper-case s) #"\s+" "%") "%"))

(defn boundary-query
  "Return a query and a crude set of coordinates reflecting a square around
  the coordinates given.
  Coordinates retuened as northing/easting/northing/easting."
  [OSNRTH1M OSEAST1M range-metres]
  (when (and range-metres (> range-metres 0))
    {:sql    "osnrth1m > ? and oseast1m > ? and osnrth1m < ? and oseast1m < ?"
     :values [(- OSNRTH1M range-metres) (- OSEAST1M range-metres)
              (+ OSNRTH1M range-metres) (+ OSEAST1M range-metres)]}))

(defn search-org-query
  "Generate a SQL vector containing SQL and parameters to search for an organisation.

  - s            : name of organisation, searching name, address town and postcode
  - only-active  : (default true) only return active organisations
  - role         : role code, e.g. RO72 for general practice.
  - OSNRTH1M     : grid coordinates if search is to be limited to a geographical region
  - OSEAST1M     : grid coordinates
  - range-metres : range from coordinates to constrain search
  - limit        : limit on number of search results"
  [{:keys [s only-active role OSNRTH1M OSEAST1M range-metres limit] :or {only-active true}}]
  (let [bq (boundary-query OSNRTH1M OSEAST1M range-metres)
        clauses (cond-> []
                        only-active (conj ["active=?" true])
                        bq (conj [(:sql bq) (:values bq)])
                        (not (str/blank? s)) (conj ["name like ?" (name-query s)])
                        role (conj ["? = ANY(roles)" role]))
        where-str (when (seq clauses) (apply str " where " (interpose " and " (map first clauses))))]
    (into [] (flatten [(str
                         "select o.data::varchar as org,o.osnrth1m, o.oseast1m from organisations o"
                         where-str
                         (when limit (str " limit " limit)))
                       (map second clauses)]))))

(defn search-org
  "Search for an organisation.
  Parameters:
  |- :s            : search text - will search name or address1 or town or postcode
  |- :role         : role code, optional, e.g. RO72 for GP surgery
  |- :only-active  : default true, whether to only include active organisations
  |- :OSNRTH1M     : northing UK grid reference on which to centre search
  |- :OSEAST1M     : easting UK grid reference on which to centre search
  |- :range-metres : if given, results will be limited to within this range. "
  [{:keys [s only-active role OSNRTH1M OSEAST1M range-metres limit] :as params}]
  (println "searching for organisation: " params)
  (let [calculate-distances? (and OSNRTH1M OSEAST1M (pos? OSNRTH1M) (pos? OSEAST1M))
        filter-range-fn (if (and calculate-distances? range-metres (pos-int? range-metres))
                          #(< (:distance-from %) range-metres)
                          #(some? %))
        result (->> (jdbc/execute! @datasource (search-org-query params))
                    (map #(-> (json/read-str (:org %) :key-fn keyword)
                              (assoc-in [:location :OSNRTH1M] (:organisations/osnrth1m %))
                              (assoc-in [:location :OSEAST1M] (:organisations/oseast1m %)))))]
    (println "calculate distances? " calculate-distances?)
    (if calculate-distances?
      (->> result
           (map #(assoc % :distance-from (postcode/distance-between params (:location %))))
           (filter filter-range-fn)
           (sort-by :distance-from))
      result)))

(defn fetch-general-practitioners-for-org
  ([id] (fetch-general-practitioners-for-org " 2.16.840.1.113883.2.1.3.2.4.18.48 " id))
  ([root id]
   (let [results (jdbc/execute! @datasource ["select data::varchar as gp from general_practitioners where organisation = ? "
                                             (str root " | " id)])]
     (map #(json/read-str (:gp %) :key-fn keyword) results))))

(defn fetch-general-practitioner
  [id]
  (when-let [gp (:gp (jdbc/execute-one! @datasource
                                        ["SELECT data::varchar as gp FROM general_practitioners WHERE id = ? "
                                         id]))]
    (json/read-str gp :key-fn keyword)))

(defn fetch-code [id]
  (jdbc/execute-one! @datasource
                     [" SELECT id, display_name, code_system FROM codes where id = ? " id]))


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
  (with-open [ps (jdbc/prepare conn ["insert into organisations (id,name,active,roles,data,osnrth1m, oseast1m)
  select ? as id,? as name,? as active,? as roles,?::jsonb as data, (pc.data->>'OSNRTH1M')::integer as osnrth1m, (pc.data->>'OSEAST1M')::integer as oseast1m from postcodes pc where PCD2 = ?
  on conflict (id) do update set name = EXCLUDED.name, active= EXCLUDED.active, roles = EXCLUDED.roles, data = EXCLUDED.data,osnrth1m = EXCLUDED.osnrth1m, oseast1m = EXCLUDED.oseast1m"])]
    (let [v (map #(vector
                    (str (get-in % [:orgId :root]) "|" (get-in % [:orgId :extension]))
                    (str (:name %) " " (get-in % [:location :address1]) " " (get-in % [:location :town]) " " (get-in % [:location :postcode]))
                    (:active %)
                    (into-array String (map :id (filter :active (:roles %))))
                    (json/write-str %)
                    (get-in % [:location :postcode])) orgs)]
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

  (migratus/create config "add-grid-ref-columns")

  ;list pending migrations
  (migratus/pending-list config)

  ;apply pending migrations
  (migratus/migrate config)

  (migratus/rollback config)

  (connection-pool-start {:dbtype          "postgresql"
                          :dbname          "ods"
                          :maximumPoolSize 10})
  (fetch-postcode "CF14 4XW")
  (fetch-org "7A4BV")
  (def ashgrove (fetch-org "2.16.840.1.113883.2.1.3.2.4.18.48" "W95024"))
  ashgrove
  (map :name (filter #(str/blank? (:leftParentDate %)) (fetch-general-practitioners-for-org "W95024")))
  (fetch-general-practitioner "G0232157")
  @datasource

  (take 5 (map :name (search-org {:s "bishop" :role "RO72"})))
  (map #(str (:name %) ":" (int (:distance-from %)) "m") (search-org (merge {:role "RO72" :range-metres 5000} (fetch-postcode "NP25 3NS"))))
  (search-org {:s "monmouth" :role "RO72" :near {:postcode "NP25 3NS" :range-metres 5000}})
  (search-org {:s "bishop" :role "RO72" :near (merge (fetch-postcode "CF14 2HB") {:range-metres 5000})})

  (connection-pool-stop)

  )
