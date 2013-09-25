(ns agent.dbadapt
    (:require
        [agent.mysqladapt :as mysql]
    )
)

(def ^:dynamic *db-func-map* mysql/mysql-map)

(defn- get-sql [connstr user pwd]
    {
        ;:subprotocol "mysql"
        :subname connstr
        :user user
        :password pwd
    }
)

(defn- get-schema-table [db tablename]
    ((:get-table-schema *db-func-map*) db tablename)
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
        ]
        (map get-schema-db dbList)
    )
)

(defn- get-db-table-list' [dbset]
    (let [db (:db dbset)
            dbname ((:get-dbname *db-func-map*) db)
            tb (:tables dbset)
            tbl (map #(hash-map % db ) tb)
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
                        (map  :tablename  (:tables db)) 
                    }
                )
                dblist
            )
        ]
        (reduce merge {} (map get-db-table-list' dbList))
    )
)