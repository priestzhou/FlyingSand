(ns agent.dbadapt
    (:require
        [agent.mysqladapt :as mysql]
    )
)

(def ^:dynamic *db-func-map* mysql/mysql-map)

(defn- get-sql 
    ([connstr user pwd]
        {
            ;:subprotocol "mysql"
            :subname connstr
            :user user
            :password pwd
        }
    )
    ([dbmap]
        (get-sql (:dbconnstr dbmap) (:dbuser dbmap) (:dbpassword dbmap))
    )
)

(defn- get-schema-table [db tablename]
    (let [table-schema ((:get-table-schema *db-func-map*) db tablename)]
        {:tablename tablename :cols table-schema}
    )
)

(defn- get-schema-db [dbset]
    (let [db (:db dbset)
            dbname ((:get-dbname *db-func-map*) db)
            tableNameList (:tables dbset)
            tbl (map (partial get-schema-table db ) tableNameList)
        ]
        {:dbname dbname :tables tbl}
    )
)

(defn get-schemas [dbsetting]
    (let [dblist (:database dbsetting)
            dbList (map 
                (fn [db]
                    {:db 
                        (get-sql 
                            (:dbconnstr db) 
                            (:dbuser db) 
                            (:dbpassword db)
                        ) 
                        :tables
                        (map  :tablename  (:tables db)) 
                    }
                )
                dblist
            )
            res (map get-schema-db dbList)
        ]
        res
    )
)

(defn- get-db-table-list' [dbset]
    (let [db (:db dbset)
            dbname ((:get-dbname *db-func-map*) db)
            tb (:tables dbset)
            tbl (map #(hash-map (:tablename %) [db %] ) tb)
            tbmap (reduce merge {} tbl )
        ]
        { dbname tbmap }
    )
)

(defn get-db-table-list [dbsetting]
    (let [dblist (:database dbsetting)
            dbList (map 
                (fn [db]
                    {:db 
                        (get-sql 
                            (:dbconnstr db) 
                            (:dbuser db) 
                            (:dbpassword db)
                        ) 
                        :tables
                        (:tables db)
                    }
                )
                dblist
            )
        ]
        (reduce merge {} (map get-db-table-list' dbList))
    )
)

(def db-table-list 
    (memoize get-db-table-list)
)

(defn- check-meta [dbs qstr]
    (let [tbl (db-table-list dbs)
            str-list (clojure.string/split qstr #"\.")
            [app version db table] str-list 
        ]
        (cond
            (not (= app (:app dbs))) {:errCode "app mismach"}
            (not (= version (:appversion dbs))) {:errCode "version mismach"}
            (nil? (find tbl db))  {:errCode "db not find"}
            (nil? (get-in tbl [db table]))  {:errCode "table not find"}
            :else {:status "sucess" 
                    :db (first (get-in tbl [db table])) 
                    :table table
                    :key (:timestampCol (second (get-in tbl [db table])))
                }
        )
    )
)

(defn get-table-all-data [dbsetting qstr]
    (let [flag (check-meta dbsetting qstr)
        ]
        (if (nil? (:errCode flag))
            { 
                :data 
                ((:get-table-all-data *db-func-map*) (:db flag) (:table flag))
            }
            flag
        )
    )
)

(defn get-table-inc-data [dbsetting qstr qnum]
    (let [flag (check-meta dbsetting qstr)
        ]
        (if (nil? (:errCode flag))
            { 
                :data 
                (
                    (:get-table-inc-data *db-func-map*) 
                    (:db flag) 
                    (:table flag) 
                    (:key flag)
                    qnum
                )
            }
            flag
        )
    )
)
