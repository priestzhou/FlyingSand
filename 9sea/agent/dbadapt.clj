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
            dbname ((:get-dbname *db-func-map*) db) 
        ]

        dbname
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