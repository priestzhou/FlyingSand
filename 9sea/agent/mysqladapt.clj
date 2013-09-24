(ns agent.mysqladapt
    (:require
        [clojure.java.jdbc :as jdbc]
        [clojure.java.jdbc.sql :as sql]
    )
)

(defn- get-db [db]
    (println  " get-db sf")
    (assoc db :subprotocol "mysql")
)

(defn- get-dbname [db]
    (println "dbname ")
    (let [mysqldb (get-db db)
            sql (str "select database();")
            t1 (println  " dbname = " sql )
        ]
        (println "mysql dbname")
        (jdbc/query mysqldb sql)
        (println "mysql dbname2")
    )
)

(def mysql-map 
    {:get-dbname get-dbname}
)