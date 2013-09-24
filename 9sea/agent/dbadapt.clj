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

(defn- get-schema-db [dbset]
    (let [db (:db dbset)
            t1 (println "get-schema-db " db)
            dbname ((:get-dbname *db-func-map*) db)       
            t3 (println dbname)
        ]
        dbname
    )
)

(defn get-schemas [dbsetting]
    (let [dblist (:database dbsetting)
            t1 (println "get-schemas 1" dblist)
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
            t2 (println "get-schemas 2" dbList)
        ]
        (map get-schema-db dbList)
    )
)