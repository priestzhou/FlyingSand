(ns agent.mysqladapt
    (:require
        [clojure.java.jdbc :as jdbc]
        [clojure.java.jdbc.sql :as sql]
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
                    (doall res)
                )
            )
            (catch Exception e
                (println e)
                (println (.printStackTrace e) )
            )
        )
    )
)

(def mysql-map 
    {:get-dbname get-dbname}
)