(ns com.eldrix.clods.sql
  "A SQLite store for NHS ODS data."
  (:require
    [clojure.core.async :as async]
    [clojure.spec.alpha :as s]
    [clojure.java.io :as io]
    [clojure.set :as set]
    [clojure.string :as str]
    [com.eldrix.clods.download :as dl]
    [com.eldrix.nhspd.api :as nhspd]
    [honey.sql :as sql]
    [honey.sql.helpers :as h]
    [next.jdbc :as jdbc]
    [next.jdbc.connection :as conn]
    [next.jdbc.date-time]
    [next.jdbc.result-set :as rs])
  (:import
    (com.zaxxer.hikari HikariDataSource)
    (java.time Instant LocalDate LocalTime ZoneOffset)))

(set! *warn-on-reflection* true)

(def ^String hl7-oid-health-and-social-care-organisation-identifier
  "HL7 OID representing a HealthAndSocialCareOrganisationIdentifier"
  "2.16.840.1.113883.2.1.3.2.4.18.48")

(def version 1)

(defn get-version
  "Return an integer representing the user_version pragma."
  [conn]
  (:user_version (jdbc/execute-one! conn ["PRAGMA user_version"])))

(defn set-version!
  [conn version]
  (jdbc/execute! conn [(str "PRAGMA user_version=" version)]))

;;
;; DDL statements
;;

(def create-schema
  ["create table if not exists release
    (id text not null, release_date text not null)"

   "create table if not exists manifest
    (version text not null, publication_type text not null,
    publication_date date not null, content_description text not null, record_count int)"

   "create table if not exists codesystem
     (id text not null, display_name text not null, code_system text not null,
      primary key(code_system, id))"

   "create table if not exists organisation
    (org_code text primary key, name text not null,
    org_record_class text not null, active boolean not null,
    address1 text, address2 text, town text, county text, country text not null,
    postcode text, uprn int, osnrth1m int, oseast1m int)"

   "create table if not exists succession
    (unique_succ_id int primary key, predecessor_org_code text not null,
    successor_org_code text not null, primary_role text,
    foreign key(predecessor_org_code) references organisation(org_code),
    foreign key(successor_org_code) references organisation(org_code))"

   "create table if not exists relationship
    (unique_rel_id int primary key, source_org_code text not null,
    id varchar(7) not null, target_org_code text not null,
    foreign key(source_org_code) references organisation(org_code),
    foreign key(target_org_code) references organisation(org_code))"

   "create table if not exists role
   (unique_role_id int primary key, org_code text not null,
   id text not null, is_primary bool, active bool,
   start_date date, end_date date,
   foreign key(org_code) references organisation(org_code))"])

(def drop-schema
  ["drop table if exists manifest"
   "drop table if exists role"
   "drop table if exists succession"
   "drop table if exists relationship"
   "drop table if exists organisation"
   "drop table if exists codesystem"
   "drop table if exists release"])

(def add-indexes
  ["create index if not exists organisation_idx_coords on organisation(osnrth1m, oseast1m)"
   "create index if not exists organisation_idx_name on organisation(name collate nocase)"
   "create index if not exists succession_idx_predecessor on succession(predecessor_org_code)"
   "create index if not exists succession_idx_successor on succession(successor_org_code)"
   "create index if not exists role_idx_org_code on role(org_code)"
   "create index if not exists role_idx_role_id on role(id)"
   "create index if not exists relationship_idx_source_org_code on relationship(source_org_code)"
   "create index if not exists relationship_idx_target_org_code on relationship(target_org_code)"
   "create index if not exists relationship_idx_children on relationship(source_org_code, id)"
   "create virtual table search using fts5 (org_code, name, address1, address2, town, county, postcode, content=\"organisation\")"
   "insert into search (org_code, name, address1, address2, town, county, postcode) select org_code,name,address1,address2,town,county,postcode from organisation"])

(def drop-indexes
  ["drop index organisation_idx_coords"
   "drop index organisation_idx_name"
   "drop index role_idx_org_code"
   "drop index role_idx_role_id"
   "drop index relationship_idx_source_org_code"
   "drop index relationship_idx_target_org_code"
   "drop index relationship_idx_children"
   "drop table search"])

(defn up! [conn]
  (doseq [stmt create-schema]
    (jdbc/execute! conn [stmt])))

(defn down! [conn]
  (doseq [stmt drop-schema]
    (jdbc/execute! conn [stmt])))

(defn add-indexes! [conn]
  (doseq [stmt add-indexes]
    (jdbc/execute! conn [stmt])))

(defn drop-indexes! [conn]
  (doseq [stmt drop-indexes]
    (jdbc/execute! conn [stmt])))

(defn optimize-writes!
  [conn]
  (jdbc/execute! conn ["pragma synchronous=off"])
  (jdbc/execute! conn ["pragma journal_mode=memory"]))

(defn optimize!
  [conn]
  (jdbc/execute! conn ["pragma optimize"]))

(defn date->time_t
  [^LocalDate d]
  (when d (.toEpochSecond d LocalTime/NOON ZoneOffset/UTC)))

(defn sdate->time_t
  "Convert a string containing an ISO date into unix time (epoch seconds)."
  [s]
  (when s (date->time_t (LocalDate/parse s))))

(defn time_t->date
  "Convert unix time (epoch seconds) to a local date"
  [epoch-seconds]
  (when epoch-seconds (.toLocalDate (.atZone (Instant/ofEpochSecond epoch-seconds) ZoneOffset/UTC))))

;;
;; insert
;;

(defn insert-release-sql
  [{:keys [id releaseDate]}]
  {:stmt "insert into release (id, release_date) values (?,?)"
   :data [[id (date->time_t releaseDate)]]})                ;; the release date is already a java.time.LocalDate as it comes from TRUD service

(defn insert-manifests-sql
  [manifests]
  {:stmt "insert into manifest (version, publication_type, publication_date, content_description, record_count) values (?,?,?,?,?)"
   :data (map (fn [{:keys [version publicationType publicationDate contentDescription recordCount]}]
                (vector version publicationType (sdate->time_t publicationDate) contentDescription recordCount))
              manifests)})

(defn insert-codesystems-sql
  [codesystems]
  {:stmt "insert into codesystem (id, display_name, code_system) values (?,?,?)
          on conflict (id, code_system) do update set display_name=excluded.display_name"
   :data (map (fn [{:keys [id displayName codeSystem]}]
                (vector id displayName codeSystem))
              codesystems)})

(defn successor->row
  [predecessor-org-id {:keys [uniqueSuccId date type target primaryRole]}]
  (let [{:keys [root extension]} target]
    [uniqueSuccId predecessor-org-id extension primaryRole]))

(defn org-successors->rows
  [{:keys [orgId successors] :as org}]
  (let [{:keys [root extension]} orgId]
    (map #(successor->row extension %) successors)))

(defn insert-orgs-successors-sql
  [orgs]
  {:stmt "insert into succession (unique_succ_id, predecessor_org_code,successor_org_code, primary_role) values (?,?,?,?)
  on conflict (unique_succ_id)
  do update set predecessor_org_code = excluded.predecessor_org_code,
                successor_org_code   = excluded.successor_org_code,
                primary_role         = excluded.primary_role"
   :data (mapcat org-successors->rows orgs)})

(defn relationship->row
  [org-code {:keys [uniqueRelId id startDate endDate active target]}]
  [uniqueRelId org-code id (:extension target)])

(defn org-rels->rows
  [{:keys [orgId relationships]}]
  (let [{:keys [root extension]} orgId]
    (map #(relationship->row extension %) relationships)))

(defn insert-orgs-rels-sql
  [orgs]
  {:stmt "insert into relationship (unique_rel_id, source_org_code, id, target_org_code) values (?,?,?,?)
   on conflict(unique_rel_id)
   do update set source_org_code    = excluded.source_org_code,
                 id                 = excluded.id,
                 target_org_code    = excluded.target_org_code"
   :data (mapcat org-rels->rows orgs)})

(defn role->row
  [org-code {:keys [uniqueRoleId id isPrimary active startDate endDate]}]
  [uniqueRoleId org-code id isPrimary active (sdate->time_t startDate) (sdate->time_t endDate)])

(defn org-roles->rows
  [{:keys [orgId roles]}]
  (let [{:keys [root extension]} orgId]
    (map #(role->row extension %) roles)))

(defn insert-orgs-roles-sql
  [orgs]
  {:stmt "insert into role (unique_role_id, org_code, id, is_primary, active, start_date, end_date) values (?,?,?,?,?,?,?)
  on conflict(unique_role_id)
  do update set org_code     = excluded.org_code,
                id           = excluded.id,
                is_primary   = excluded.is_primary,
                active       = excluded.active,
                start_date   = excluded.start_date,
                end_date     = excluded.end_date"
   :data (mapcat org-roles->rows orgs)})

(defn org->row
  [nhspd {:keys [orgId name operational orgRecordClass active primaryRole location isReference] :as org}]
  (when isReference
    (throw (ex-info "Only 'full' organisation records should be written to store" org)))
  (let [{:keys [root extension]} orgId
        {:keys [start end]} operational
        {:keys [address1 address2 town county postcode country uprn]} location
        {:keys [OSNRTH1M OSEAST1M]} (nhspd/postcode nhspd postcode)] ;; could derive coords from uprn if we had a UPRN service?
    (when (not= hl7-oid-health-and-social-care-organisation-identifier root)
      (throw (ex-info "Organisation does not have standard HL7 OID; implicit assumption that all organisations have this root failed" org)))
    [extension name (clojure.core/name orgRecordClass) active address1 address2 town county country postcode uprn OSNRTH1M OSEAST1M]))

(defn insert-orgs-sql
  [nhspd orgs]
  {:stmt "insert into organisation (org_code, name, org_record_class, active, address1, address2, town, county, country, postcode, uprn, osnrth1m, oseast1m)
          values (?,?,?,?,?,?,?,?,?,?,?,?,?)
          on conflict(org_code)
          do update set name             = excluded.name,
                        org_record_class = excluded.org_record_class,
                        active           = excluded.active,
                        address1         = excluded.address1,
                        address2         = excluded.address2,
                        town             = excluded.town,
                        county           = excluded.county,
                        country          = excluded.country,
                        postcode         = excluded.postcode,
                        uprn             = excluded.uprn,
                        osnrth1m         = excluded.osnrth1m,
                        oseast1m         = excluded.oseast1m"
   :data (map #(org->row nhspd %) orgs)})

(defn write-batch!
  [conn {:keys [stmt data]}]
  (jdbc/execute-batch! conn stmt data {}))

(defn write-distribution!
  [conn nhspd {:keys [manifests code-systems organisations release paths] :as dist}]
  (write-batch! conn (insert-release-sql release))
  (write-batch! conn (insert-manifests-sql manifests))
  (write-batch! conn (insert-codesystems-sql (vals code-systems)))
  (loop [orgs (async/<!! organisations)]
    (when (seq orgs)
      (jdbc/with-transaction [txn conn]
        (write-batch! txn (insert-orgs-sql nhspd orgs))
        (write-batch! txn (insert-orgs-successors-sql orgs))
        (write-batch! txn (insert-orgs-rels-sql orgs))
        (write-batch! txn (insert-orgs-roles-sql orgs)))
      (recur (async/<!! organisations)))))

;;
;; read
;;

(defn execute!
  [connectable sql-params]
  (jdbc/execute! connectable sql-params {:builder-fn rs/as-unqualified-maps}))

(defn execute-one!
  [connectable sql-params]
  (jdbc/execute-one! connectable sql-params {:builder-fn rs/as-unqualified-maps}))

(defn normalize-manifest [m]
  (update m :publicationdate #(some-> % LocalDate/parse)))

(defn normalize-codesystem [{:keys [id code_system display_name]}]
  {:id id :code (subs id 2) :displayName display_name :codeSystem code_system})

(defn normalize-bool [s]
  (= 1 s))

(defn normalize-role
  [{:keys [active id is_primary start_date end_date]}]
  {:id        id
   :active    (normalize-bool active)
   :isPrimary (normalize-bool is_primary)
   :startDate (time_t->date start_date)
   :endDate   (time_t->date end_date)})

(defn normalize-rel
  [{:keys [id target_org_code]}]
  {:id     id
   :target {:root hl7-oid-health-and-social-care-organisation-identifier, :extension target_org_code}})

(defn normalize-successor
  [{:keys [primary_role successor_org_code]}]
  {:primaryRole primary_role
   :target      {:root hl7-oid-health-and-social-care-organisation-identifier, :extension successor_org_code}})

(defn normalize-predecessor
  [{:keys [primary_role predecessor_org_code]}]
  {:primaryRole primary_role
   :target      {:root hl7-oid-health-and-social-care-organisation-identifier, :extension predecessor_org_code}})

(defn manifests
  [conn]
  (map normalize-manifest (execute! conn ["select * from manifest"])))

(defn codesystems
  "Return codes grouped by a vector of OID and id to facilitate arbitrary lookup.
  ```
  (get (codesystems conn) [\"2.16.840.1.113883.2.1.3.2.4.17.508\" \"RE8\"])
  ``` 
  Each code will include 
  - id   : e.g. \"RE8\"
  - code : e.g. \"8\"
  - displayName : e.g .\"IS PARTNER TO\"."
  [conn]
  (->> (execute! conn ["select * from codesystem"])
       (map normalize-codesystem)
       (reduce (fn [acc {:keys [id codeSystem] :as m}]
                 (assoc acc [codeSystem id] m)) {})))

(defn roles [conn]
  (->> (execute! conn ["select * from codesystem where code_system='2.16.840.1.113883.2.1.3.2.4.17.507'"])
       (map normalize-codesystem)))

(defn relationships [conn]
  (->> (execute! conn ["select * from codesystem where code_system='2.16.840.1.113883.2.1.3.2.4.17.508'"])
       (map normalize-codesystem)))

(defn record-classes [conn]
  (->> (execute! conn ["select * from codesystem where code_system='2.16.840.1.113883.2.1.3.2.4.23'"])
       (map normalize-codesystem)))

(defn normalize-org
  "Normalises raw row data from database into a nested organisation structure"
  [{:keys [org_code org_record_class address1 address2 town county country postcode uprn osnrth1m oseast1m distance] :as org}]
  (-> org
      (dissoc :org_code :address1 :address2 :town :county :country :postcode :uprn :osnrth1m :oseast1m :distance)
      (update :active normalize-bool)
      (assoc :orgId {:root hl7-oid-health-and-social-care-organisation-identifier :extension org_code}
             :orgRecordClass (keyword org_record_class)
             :location (cond-> {:address1 address1, :address2 address2, :town town :county county :country country :postcode postcode :uprn uprn :osnrth1m osnrth1m :oseast1m oseast1m}
                         distance                           ;; iff there is 'distance' (when geosearching), include it
                         (assoc :distance distance)))))

(defn extended-org
  "Extends a normalized org to include important relationships including roles,
  and succession."
  [conn {:keys [orgId] :as org}]
  (let [extension (:extension orgId)
        roles (map normalize-role (execute! conn ["select id, active,is_primary,start_date,end_date from role where org_code=?" extension]))
        successors (map normalize-successor (execute! conn ["select primary_role, successor_org_code from succession where predecessor_org_code=?" extension]))
        predecessors (map normalize-predecessor (execute! conn ["select primary_role, predecessor_org_code from succession where successor_org_code=?" extension]))]
    (-> org
        (assoc :roles roles
               :primaryRole (reduce (fn [_ {:keys [isPrimary] :as role}] (when isPrimary (reduced role))) nil roles)
               :relationships (map normalize-rel (jdbc/execute! conn ["select id, target_org_code from relationship where source_org_code=?" extension] {:builder-fn rs/as-unqualified-maps})))
        (cond->
          (seq successors)
          (assoc :successors successors)
          (seq predecessors)
          (assoc :predecessors predecessors)))))

(defn fetch-org
  [conn extension]
  (when-let [org (execute-one! conn ["select * from organisation where org_code=?" extension])]
    (extended-org conn (normalize-org org))))

(defn fetch-orgs
  "Fetch multiple organizations by their codes.
  Returns a sequence of organization maps with all related data."
  [conn org-codes]
  (when (seq org-codes)
    (let [orgs (jdbc/execute! conn
                              (sql/format {:select :*
                                           :from :organisation
                                           :where [:in :org_code org-codes]})
                              {:builder-fn rs/as-unqualified-maps})]
      (map #(extended-org conn (normalize-org %)) orgs))))

(defn random-orgs
  "Return 'n' random organisations. Only for use in testing against a live database."
  [conn n]
  (map #(extended-org conn (normalize-org %))
       (execute! conn ["select * from organisation ORDER BY RANDOM() LIMIT ?" n])))

(defn all-predecessors-sql
  [org-code]
  ["with recursive predecessors(org_code) as
  (select predecessor_org_code from succession where successor_org_code=?
   union all
   select predecessor_org_code from succession, predecessors where succession.successor_org_code=predecessors.org_code)
  select org_code from predecessors" org-code])

(defn all-predecessors
  "Return a set of organisation codes of predecessor organisations."
  [conn org-code]
  (into #{} (map :org_code) (jdbc/plan conn (all-predecessors-sql org-code))))

(defn all-successors-sql
  [org-code]
  ["with recursive successors(org_code) as
  (select successor_org_code from succession where predecessor_org_code=?
   union all
   select successor_org_code from succession, successors where succession.predecessor_org_code=successors.org_code)
  select org_code from successors" org-code])

(defn all-successors
  "Return a set of organisation codes of successor organisations."
  [conn org-code]
  (into #{} (map :org_code) (jdbc/plan conn (all-successors-sql org-code))))

(defn active-successors-sql
  [org-code]
  ["with recursive successors(org_code) as
  (select successor_org_code from succession where predecessor_org_code=?
   union all
   select successor_org_code from succession, successors where succession.predecessor_org_code=successors.org_code)
  select s.org_code from successors s
  join organisation o on s.org_code = o.org_code
  where o.active = 1" org-code])

(defn active-successors
  "Return a set of organisation codes for active successor organisations only.
  This recursively traverses the succession chain and filters for currently active
  organisations. For example, if A → B (inactive) → C (active), this returns #{C}
  when called with A."
  [conn org-code]
  (into #{} (map :org_code) (jdbc/plan conn (active-successors-sql org-code))))

(defn active-successors-batch-sql
  [org-codes]
  (sql/format
   {:with-recursive [[[:successors {:columns [:org_code]}]
                     {:union-all [{:select :successor_org_code
                                   :from :succession
                                   :where [:in :predecessor_org_code org-codes]}
                                  {:select :succession.successor_org_code
                                   :from [:succession :successors]
                                   :where [:= :succession.predecessor_org_code :successors.org_code]}]}]]
    :select-distinct :s.org_code
    :from [[:successors :s]]
    :join [:organisation [:= :s.org_code :organisation.org_code]]
    :where [:= :organisation.active 1]}))

(defn active-successors-batch
  "Return a set of active successor organisation codes for multiple org codes.
  Returns the union of all active successors across all input organisations.
  For example, if called with [A B] where A → C (active) and B → D (active),
  returns #{C D}."
  [conn org-codes]
  (when (seq org-codes)
    (into #{} (map :org_code) (jdbc/plan conn (active-successors-batch-sql org-codes)))))

(defn equivalent-org-codes
  "Returns a set of predecessor and successor organisation codes. Set will include
  the original organisation code. Unlike `all-equivalent-orgs` this will *not* return
  the same result and will depend on the starting organisation.
  ```
  (= (equivalent-orgs conn \"RWM\") (equivalent-orgs conn \"7A4\"))
  => false
  ```"
  [conn org-code]
  (set/union (all-predecessors conn org-code)
             (all-successors conn org-code)
             #{org-code}))

(defn all-equivalent-org-codes
  "Returns a set of equivalent organisation codes by looking at the successors, and
  then returning those and all predecessors. In this way, this returns the same
  result for any organisation within that set.
  ```
  (= (all-equivalent-orgs conn \"RWM\") (all-equivalent-orgs conn \"7A4\"))
  => true
  ```"
  [conn org-code]
  (into #{}
        (mapcat #(all-predecessors conn %))
        (conj (all-successors conn org-code) org-code)))

(defn proximal-parent-org-codes
  ([conn org-code]
   (into #{} (map :target_org_code)
         (jdbc/plan conn ["select target_org_code from relationship where source_org_code=?" org-code])))
  ([conn org-code rel-types]
   (into #{} (map :target_org_code)
         (jdbc/plan conn (sql/format {:select :target_org_code
                                      :from   :relationship
                                      :where  [:and [:= :source_org_code org-code]
                                               [:in :id rel-types]]})))))

(defn proximal-child-orgs-sql
  "Return a SQL query to return proximal child relations matching the 
  criteria specified. 
  - org-code : organisation code of the parent
  - active   : whether to only return active organisations
  - rels     : a string with a single relationship type id, or collection of strings
  - roles    : a string with a single role type id, or collection of strings"
  [org-code {:keys [active rels roles]}]
  (cond-> {:select :source_org_code
           :from   :relationship
           :where  [:= :target_org_code org-code]}
    (string? rels) (h/where := :id rels)
    (coll? rels) (h/where :in :id rels)
    (string? roles) (-> (h/join :role [:= :role/org_code :source_org_code])
                        (h/where := :role/id roles))
    (coll? roles) (-> (h/join :role [:= :role/org_code :source_org_code])
                      (h/where :in :role/id roles))
    (and roles active) (h/where := :role/active 1)
    (and (empty? roles) active) (-> (h/join :organisation [:= :organisation/org_code :source_org_code])
                                    (h/where := :organisation/active 1))))

(defn proximal-child-org-codes
  "Return proximal child organisations.
  Parameters:
  - org-code      : ODS organisation code, e.g. \"7A4\"
  - active        : whether to only include active child organisations
  - rels          : string or collection of relationship types e.g. \"RE6\"
  - roles         : string or collection of role types e.g. \"RO177\"

  So if you want all active GP surgeries within a specified health board:
  ```
  (map #(select-keys (fetch-org conn %) [:name :orgId :active])
       (proximal-child-org-codes conn \"7A4\" {:roles \"RO177\" :rels #{\"RE6\"} :active true}))
  ```
  This will only return active organisations with a relationship RE6 (operated
  by) the parent organisation '7A4' with role RO177 (prescribing cost centre)."
  ([conn org-code]
   (into #{} (map :source_org_code)
         (jdbc/plan conn ["select source_org_code from relationship where target_org_code=?" org-code])))
  ([conn org-code params]
   (into #{} (map :source_org_code)
         (jdbc/plan conn (sql/format (proximal-child-orgs-sql org-code params))))))

(defn all-child-orgs-sql
  ([org-code]
   ["with recursive children(org_code) as (select source_org_code from relationship where target_org_code=? union all select source_org_code from relationship, children where relationship.target_org_code=children.org_code) select org_code from children" org-code])
  ([org-code rel-type-ids]
   (sql/format {:with-recursive [[[:children {:columns [:org_code]}]
                                  {:union-all [{:select :source_org_code :from :relationship
                                                :where  [:and [:= :target_org_code org-code]
                                                         [:in :id rel-type-ids]]}
                                               {:select :source_org_code :from [:relationship :children]
                                                :where  [:and [:= :relationship.target_org_code :children.org_code]
                                                         [:in :id rel-type-ids]]}]}]]
                :select         :org_code :from :children})))

(defn all-child-org-codes
  ([conn org-code]
   (into #{} (map :org_code) (jdbc/plan conn (all-child-orgs-sql org-code))))
  ([conn org-code rel-type-ids]
   (into #{} (map :org_code) (jdbc/plan conn (all-child-orgs-sql org-code rel-type-ids)))))

(defn orgs-with-primary-role
  "Returns organisations with one of the roles specified."
  ([conn role-or-roles org-codes]
   (into #{}
         (map :org_code)
         (jdbc/plan conn
                    (sql/format {:select :org_code :from :role
                                 :where  [:and [:= :is_primary 1]
                                          (if (coll? role-or-roles) [:in :id role-or-roles]
                                                                    [:= :id role-or-roles])
                                          [:in :org_code org-codes]]})))))

(defn q-org-name
  "Update the query to limit to organisations with the specified name."
  [query nm]
  (if-not (str/blank? nm)
    (h/where query [:in :organisation/org_code [[:raw "select org_code from search where name match " [:lift nm]]]])
    query))

(defn q-org-s
  "Update the query to limit to organisations matching search string 's'. This will search
  columns org_code, name, address1, address2, town, county and postcode."
  [query s]
  (if-not (str/blank? s)
    (h/where query [:in :organisation/org_code [[:raw "select org_code from search where search match " [:lift s]]]])
    query))

(defn q-org-address
  [query s]
  (if-not (str/blank? s)                                    ;; define fts fields to search....
    (let [s' (str "{address1 address2 town county postcode} : (" s ")")]
      (h/where query [:in :organisation/org_code [[:raw "select org_code from search where search match " [:lift s']]]]))
    query))

(defn q-org-active
  "Update the query to limit to only active organisations."
  [query]
  (h/where query [:= :organisation/active 1]))

(defn q-org-rc
  "Update a query to limit to only organisations of the specified record 
  class."
  [query rc]
  (h/where query [:= :organisation/org_record_class rc]))

(defn q-org-role
  [query role]
  (h/join query :role [:and
                       [:= :organisation/org_code :role/org_code]
                       [:= :role/id role]]))
(defn q-org-roles
  "Update the query to limit to organisations with one of the specified roles.
  Note: this may return multiple rows per organisation if an organisation has
  multiple roles and 'only-primary' is false. It would usually be more
  appropriate to use [[q-org-primary-role]]."
  ([query roles]
   (q-org-roles query roles {}))
  ([query roles {:keys [only-primary] :or {only-primary true}}]
   (h/join query :role [:and
                        [:= :organisation/org_code :role/org_code]
                        [:in :role/id roles]
                        (when only-primary [:= :role/is_primary 1])])))

(defn q-org-primary-role
  "Update the query to limit to organisations with a primary role of 'role'."
  [query role]
  (h/join query :role [:and
                       [:= :organisation/org_code :role/org_code]
                       [:= :role/id role]
                       [:= :role/is_primary 1]]))

(defn q-org-primary-roles
  "Update the query to limit to organisations with a primary role of 'roles'."
  [query roles]
  (h/join query :role [:and
                       [:= :organisation/org_code :role/org_code]
                       [:in :role/id roles]
                       [:= :role/is_primary 1]]))

(defn q-org-within-distance
  "Update the query to limit to organisations within 'd' metres of the
  coordinates specified. This uses a crude Pythagoras calculation and optimises
  by specifying a rectangular bounding box to limit on-the-fly calculations."
  [query osnrth1m oseast1m d]
  (-> query
      (h/select [[:round [:sqrt [:+ [:pow [:- :oseast1m oseast1m] 2] [:pow [:- :osnrth1m osnrth1m] 2]]]] :distance])
      (h/where :and
               [:> :osnrth1m (- osnrth1m d)] [:< :osnrth1m (+ osnrth1m d)]
               [:> :oseast1m (- oseast1m d)] [:< :oseast1m (+ oseast1m d)]
               [:< :distance d])))

(defn q-org-near-postcode
  "Convenience query to search 'd' metres within a postcode."
  [query nhspd postcode d]
  (if-let [{:keys [OSNRTH1M OSEAST1M]} (nhspd/postcode nhspd postcode)]
    (q-org-within-distance query OSNRTH1M OSEAST1M d)
    (throw (ex-info "postcode not found" {:postcode postcode}))))

(defn q-org-order-by-distance
  "Update the query to order by distance ascending. Must be used in conjunction
  with `q-org-select-distance`."
  [query]
  (assoc query :order-by [[:distance :asc]]))

(defn q-org-child-of
  [query {:keys [org-code] :as params}]
  (h/where query :in :organisation/org_code (proximal-child-orgs-sql org-code params)))

(defn escape-fts-string
  "Given user entered string 's' return a string suitable for fts matching."
  [s]
  (->> (str/split (str/lower-case s) #"\W")
       (remove str/blank?)
       (map #(str % "*"))
       (str/join " ")))

(defn make-search-query
  "Create a search query for an organisation / site.
  Parameters:
  - :s             : search for name or address of organisation
  - :n             : search for name of organisation
  - :address       : search within address of organisation
  - :active        : only include active organisations (default, true)
  - :roles         : a string or vector of roles e.g. \"RO87\"
  - :primary-role  : a string or vector of roles e.g. \"RO177\"
  - :rc            : limit to a record class e.g. \"RC1\" 'organisation' and \"RC2\" for 'site'
  - :child-of      : a map containing :org-code of parent organisation and
                     optionally including :rels, :roles and :active
  - :from-location : a map containing:
           - :osnrth1m  : OS northing
           - :oseast1m  : OS easting
           - :range     : range in metres, default 5000
  - limit         : limit on number of search results"
  [query {:keys [s n address active child-of from-location roles primary-role rc limit] :or {active true}}]
  (let [{:keys [osnrth1m oseast1m range] :or {range 5000}} from-location]
    (cond-> (or query {:select :organisation/org_code :from :organisation})
      s (q-org-s (escape-fts-string s))
      n (q-org-name (escape-fts-string n))
      address (q-org-address address)
      active (q-org-active)
      (string? roles) (q-org-role roles)
      (coll? roles) (q-org-roles roles)
      (string? primary-role) (q-org-primary-role primary-role)
      (coll? primary-role) (q-org-primary-roles primary-role)
      rc (q-org-rc rc)
      child-of (q-org-child-of child-of)
      (and osnrth1m oseast1m range) (-> (q-org-within-distance osnrth1m oseast1m range)
                                        (q-org-order-by-distance))
      limit (h/limit limit))))

(defn search
  "Perform a search for an organisation using the search parameters specified.
  By default, returns as a set of organisation codes, but can return a sequence
  of sorted codes (distance from 'from-location'), organisations or extended
  organisations depending on ':as' parameter."
  [conn {:keys [as] :as params :or {as :codes}}]
  (case as
    :orgs
    (map normalize-org
         (execute! conn (sql/format (make-search-query {:select :organisation/* :from :organisation} params))))

    :ext-orgs
    (map #(extended-org conn (normalize-org %))
         (execute! conn (sql/format (make-search-query {:select :organisation/* :from :organisation} params))))

    :codes
    (into #{} (map :org_code)
          (jdbc/plan conn (sql/format (make-search-query {:select :organisation/org_code :from :organisation} params))))

    :sorted-codes
    (into [] (map :org_code)
          (jdbc/plan conn (sql/format (make-search-query {:select :organisation/org_code :from :organisation} params))))

    ;; unsupported :as
    (throw (ex-info (str "Unsupported search return type '" as "'")
                    {:actual as, :expected #{:orgs :ext-orgs :codes :sorted-codes}}))))

(s/def ::f some?)
(s/def ::manifests some?)
(s/def ::code-systems some?)
(s/def ::organisations some?)
(s/def ::release some?)
(s/def ::dist (s/keys :req-un [::manifests ::code-systems ::organisations ::release]))
(s/def ::create-db-params
  (s/keys :req-un [::f ::dist ::nhspd]))

(defn create-db
  "Create an ODS database 'f' from distribution 'dist'"
  [{:keys [f dist nhspd] :as params}]
  (when-not (s/valid? ::create-db-params params)
    (throw (ex-info "invalid parameters" (s/explain-data ::create-db-params params))))
  (if (.exists (io/file f))
    (throw (ex-info "file already exists" {:f f}))
    (let [ds (jdbc/get-datasource (str "jdbc:sqlite:" (.getCanonicalPath (io/file f))))]
      (with-open [conn (jdbc/get-connection ds)]
        (set-version! conn version)
        (up! conn)
        (optimize-writes! conn)
        (write-distribution! conn nhspd dist)
        (add-indexes! conn)
        (optimize! conn)))))

(defn update-db
  "Update a database in place. Data must exist. Indexes are not dropped and then regenerated
  in case of concurrent use by other connections."
  [{:keys [f dist nhspd] :as params}]
  (when-not (s/valid? ::create-db-params params)
    (throw (ex-info "invalid parameters" (s/explain-data ::create-db-params params))))
  (let [f' (io/file f)]
    (if (.exists f')
      (let [ds (jdbc/get-datasource (str "jdbc:sqlite:" (.getCanonicalPath f')))
            v (get-version ds)]
        (when (not= version v)
          (throw (ex-info "incompatible database version" {:expected version :found v})))
        (write-distribution! ds nhspd dist)
        (optimize! ds))
      (throw (ex-info (str "file not found:" f) {})))))

(defn close-ds
  "Close a datasource if it implements AutoCloseable."
  [ds]
  (when (instance? java.lang.AutoCloseable ds)
    (.close ^java.lang.AutoCloseable ds)))

(defn open-ds
  "Open a datasource for the given database file.

  Parameters (as options map):
  - :f    : path to the SQLite database file (required)
  - :pool : if true, creates a connection pool with default settings
            if a map, creates a connection pool with the specified HikariCP options
            if false/nil, creates a simple datasource without pooling

  HikariCP options when :pool is a map:
  - :maximumPoolSize    : maximum number of connections (default: 10)
  - :minimumIdle        : minimum number of idle connections (default: same as maximum)
  - :connectionTimeout  : timeout in milliseconds (default: 30000)
  - :idleTimeout        : idle timeout in milliseconds (default: 600000)
  - :maxLifetime        : max lifetime in milliseconds (default: 1800000)
  - :poolName           : name of the connection pool

  Examples:
  ```
  (open-ds {:f \"data.db\"})                                    ; simple datasource
  (open-ds {:f \"data.db\" :pool true})                         ; pooled with defaults
  (open-ds {:f \"data.db\" :pool {:maximumPoolSize 5}})        ; pooled with options
  ```

  Note: When using a pool, close it when done using with-open:
  ```
  (with-open [ds (open-ds {:f \"data.db\" :pool true})]
    ;; use ds
    )
  ```"
  [{:keys [f pool]}]
  (let [f' (io/file f)]
    (when-not (.exists f')
      (throw (ex-info (str "file not found:" f) {})))
    (if pool
      (conn/->pool HikariDataSource
                   (assoc (if (map? pool) pool {})
                          :jdbcUrl (str "jdbc:sqlite:" (.getCanonicalPath f'))))
      (jdbc/get-datasource {:dbtype "sqlite" :dbname (.getCanonicalPath f')}))))

(comment
  (def nhspd (nhspd/open "../pc4/data/nhspd-2022-11-10.db"))
  (def dist (dl/download {:api-key   (str/trim-newline (slurp "../trud/api-key.txt"))
                          :cache-dir "../trud/cache"
                          :nthreads  8 :batch-size 100}))
  (keys dist)
  (create-db {:f "wibble3.db" :dist dist :nhspd nhspd}))

(comment
  (require '[clojure.pprint :as pp])
  (make-search-query nil
                     {:as            :ext-orgs :child-of {:org-code "7A4" :rel-type-id "RE6"}
                      :from-location {:oseast1m 317268 :osnrth1m 180777 :range 5000}})
  (def conn (open-ds {:f "wibble3.db"}))
  ;; with connection pooling:
  (def pooled-conn (open-ds {:f "wibble3.db" :pool true}))
  (def pooled-conn (open-ds {:f "wibble3.db" :pool {:maximumPoolSize 5}}))
  ;; remember to close pooled connections:
  (close-ds pooled-conn)

  (get (codesystems conn) "2.16.840.1.113883.2.1.3.2.4.17.508")

  (group-by :code_system (execute! conn ["select * from codesystem"]))
  (search conn {:as            :ext-orgs :child-of {:org-code "7A4" :roles "RO177" :rels "RE6"}
                :from-location {:oseast1m 317268 :osnrth1m 180777 :range 5000}})
  (clojure.pprint/print-table (execute! conn (sql/format (make-search-query {:select [:name :address1 :address2 :town :county :country] :from :organisation} {:s "heath park" :from-location {:oseast1m 317268 :osnrth1m 180777 :range 5000}}))))
  (sql/format (q-org-within-distance {:select [:org_code :name :osnrth1m :oseast1m] :from :organisation :limit 50 :order-by [[:distance :asc]]} 179363 317518 500))
  (make-search-query nil {:child-of {:org-code "7A4"}})

  (time (clojure.pprint/print-table
          (jdbc/execute! conn (sql/format (-> {:select :* :limit 20 :from :organisation}
                                              (q-org-active)
                                              (q-org-near-postcode nhspd "NP25 3NS" 50000)
                                              #_(q-org-role "RO177") ;; prescribing cost centre -> GP
                                              (q-org-roles #{"RO177" "RO72" "RO76" "RO80" "RO82" "RO87" "RO246" "RO247" "RO248" "RO249" "RO250" "RO251" "RO252" "RO253" "RO254" "RO255" "RO259" "RO260"})
                                              (q-org-order-by-distance)))
                         {:builder-fn rs/as-unqualified-maps})))

  (require '[clojure.repl.deps :refer [add-libs]])
  (add-libs {'com.github.seancorfield/honeysql  {:mvn/version "2.6.1126"}
             'com.github.seancorfield/next.jdbc {:mvn/version "1.3.925"}
             'org.xerial/sqlite-jdbc            {:mvn/version "3.45.3.0"}
             'com.zaxxer/HikariCP               {:mvn/version "6.2.1"}})

  (def dist (dl/download {:api-key   (str/trim-newline (slurp "../trud/api-key.txt"))
                          :cache-dir "../trud/cache"
                          :nthreads  8 :batch-size 100}))
  (keys dist)
  (:manifests dist)
  (first (:code-systems dist))
  (def ch (:organisations dist))
  (def orgs (async/<!! ch))
  (loop [orgs (async/<!! ch)
         roots #{}]
    (if (seq orgs)
      (recur (async/<!! ch) (into roots (map (comp :root :orgId) orgs)))
      roots))
  (first orgs)
  (.getName (io/file "../../wibble.txt"))
  (def conn (jdbc/get-connection "jdbc:sqlite:wibble.db"))  ;; a connection cannot be shared between threads
  (def nhspd (nhspd/open "../pc4/data/nhspd-2022-11-10.db"))
  (.close conn)
  (insert-codesystems-sql (vals (:code-systems dist)))
  (insert-orgs-sql nhspd (take 1 orgs))
  (insert-orgs-rels-sql orgs)
  (insert-orgs-roles-sql orgs)
  (insert-orgs-successors-sql orgs)
  (time (write-distribution! conn nhspd dist))
  (manifests conn)
  (get-version conn)
  (set-version! conn 0)
  (down! conn)
  (drop-indexes! conn)
  (up! conn)
  (jdbc/execute! conn ["pragma synchronous=off"])
  (jdbc/execute! conn ["pragma journal_mode=memory"])
  (jdbc/execute! conn ["pragma optimize"])
  (jdbc/execute! conn ["pragma page_size"])
  (jdbc/execute! conn ["vacuum"])
  (time (add-indexes! conn))
  (time (def orgs (jdbc/execute! conn ["select org_code, oseast1m, osnrth1m, sqrt(pow(abs(oseast1m-317518),2)+pow(abs(osnrth1m-179363),2)) as distance, name from organisation where org_code like '7a4%' and active='1' order by distance asc limit 100;"])))
  (def org-code "7A4")
  (def rel-type-ids #{"RE5"})
  (all-child-orgs-sql "7A4" #{"RE3"})
  (nhspd/fetch-postcode nhspd "CF14 4XW")
  (time (count (jdbc/execute! conn (all-child-orgs-sql "7A4" #{"RE5"})))))
