(ns agent.dbadapt
    (:use 
        [logging.core :only [defloggers]]
    )        
    (:require
        [agent.mysqladapt :as mysql]
    )
)

(defloggers debug info warn error)

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

(defn- get-db-table-list'' [dbset]
    (let [db (:db dbset)
            dbname ((:get-dbname *db-func-map*) db)
            tb (:tables dbset)
            tbl (map #(hash-map (:tablename %)  % ) tb)
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


(defn- get-schema-table [db tablename]
    (let [table-schema ((:get-table-schema *db-func-map*) db tablename)]
        {:tablename tablename :cols table-schema}
    )
)

(defn- get-schema-db [dbset]
    (let [db (:db dbset)
            dbname ((:get-dbname *db-func-map*) db)
            tableNameList (map :tablename (:tables dbset))
            tbl (map (partial get-schema-table db ) tableNameList)
        ]
        {:dbname dbname :tables tbl}
    )
)

(defn get-schemas [dbsetting]
    (let [dblist (:database dbsetting)
            _ (debug "dblist1" dblist)
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
            _ (debug "dblist2" (str dbList))
            res1 (map #(merge (get-schema-db %) (get-db-table-list'' %)) dbList)
            res (map get-schema-db dbList)
        ]
        res1
    )
)


(defn- check-meta [dbs db table]
    (let [tbl (db-table-list dbs)
        ]
        (cond
            (nil? (find tbl db))  {:errCode "db not find"}
            (nil? (get-in tbl [db table]))  {:errCode "table not find"}
            :else {:status "sucess" 
                    :db (first (get-in tbl [db table])) 
                    :table table
                    :key (:timestampCol (second (get-in tbl [db table])))
                    :selcols (:selcols (second (get-in tbl [db table])))
                }
        )
    )
)

(defn get-table-all-data [dbsetting db tb]
    (let [flag (check-meta dbsetting db tb)
        ]
        (if (nil? (:errCode flag))
            { 
                :data 
                ((:get-table-all-data *db-func-map*) 
                    (:db flag) 
                    (:table flag)
                    (:selcols flag)
                )
            }
            flag
        )
    )
)

(defn- filter-key [kstr]
    (->>
        kstr
        (#(if (= \` (first %))
            (reduce str (rest %))
            %
        ))
        (#(if (= \` (last %))
            (reduce str (butlast %))
            %
        ))        
    )
)

(defn get-table-inc-data [dbsetting db tb qnum]
    (let [flag (check-meta dbsetting db tb)
        ]
        (if (nil? (:errCode flag))
            { 
                :data 
                (
                    (:get-table-inc-data *db-func-map*) 
                    (:db flag) 
                    (:table flag) 
                    (filter-key (:key flag))
                    qnum
                    (:selcols flag)
                )
            }
            flag
        )
    )
)
