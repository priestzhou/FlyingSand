(ns agent.mysqladapt
    (:require
        [clojure.java.jdbc :as jdbc]
        [clojure.java.jdbc.sql :as sql]
    )
    (:import 
        [java.sql SQLException]
    )
)

(defn- get-db [db]
    (assoc db :subprotocol "mysql" :classname "com.mysql.jdbc.Driver")
)

(defn- get-dbname [db]
    (let [mysqldb (get-db db)
            sql (str "select database();")
        ]
        ;(jdbc/query mysqldb sql)
        ;(jdbc/insert-rows mysqldb :test1 [1 "Tim"] [2 "Tom"])
        (try
            (jdbc/with-connection mysqldb
                (jdbc/with-query-results res [sql]
                    (->>
                        res
                        first
                        vals
                        first
                        doall
                    )
                )
            )
            (catch SQLException e
                (println e)
                (println (.printStackTrace e) )
            )
        )
    )
)

(defn- get-table-schema [db tn]
    (let [mysqldb (get-db db)
            sql (str "desc `" tn "` ;")
        ]
        (try
            (jdbc/with-connection mysqldb
                (jdbc/with-query-results res [sql]
                    (->>
                        res
                        doall
                    )
                )
            )
            (catch SQLException e
                (println e)
                (println (.printStackTrace e) )
            )
        )
    )
)

(defn- get-table-all-data [db tn]
    (let [mysqldb (get-db db)
            sql (str "select * from `" tn "`")
        ]
        (try
            (jdbc/with-connection mysqldb
                (jdbc/with-query-results res [sql]
                    (->>
                        res
                        doall
                    )
                )
            )
            (catch SQLException e
                (println e)
                (println (.printStackTrace e) )
            )
        )        
    )

)

(defn- get-table-inc-data [db tn qkey qnum]
    (let [mysqldb (get-db db)
            sql (str 
                    "select * from `" tn 
                    "` where `" qkey "` > '" qnum "'"
                )
        ]
        (try
            (jdbc/with-connection mysqldb
                (jdbc/with-query-results res [sql]
                    (->>
                        res
                        doall
                    )
                )
            )
            (catch SQLException e
                (println e)
                (println (.printStackTrace e) )
            )
        )        
    )

)

(def mysql-map 
    {
        :get-dbname get-dbname
        :get-table-schema get-table-schema
        :get-table-all-data get-table-all-data
        :get-table-inc-data get-table-inc-data
    }
)