(ns agent-testtool.main
    (:require
        [argparser.core :as arg]
        [clojure.java.jdbc :as jdbc]
        [clojure.java.jdbc.sql :as sql]         
    )
    (:import 
        [java.sql SQLException]
    )    
    (:gen-class)
)

(defn- getdatabse [opts]
    {
        :subname (first (:dburl opts))
        :user (first (:dbuser opts))
        :password (first (:dbpwd opts))
        :subprotocol "mysql" :classname "com.mysql.jdbc.Driver"
    }
)


(defn- get-table-max-data [opts]
    (let [mysqldb (getdatabse opts)
            sql (str 
                    "select `"(first (:id opts))
                    "`, `" (first (:time opts)) "`from `" (first(:table opts) )
                    "`  "
                    "ORDER BY `" (first (:id opts)) "` desc LIMIT 1 "
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

(defn- sql-gen-new-data? [opts ]
    (let [
            sql (str 
                    "insert into " (first(:table opts) ) " (`"(first (:id opts))
                    "`, `" (first (:time opts)) "` ) values ( ?,?) "
                )
        ]
        sql
    )
)

(defn- sql-gen-new-data [opts newid newtime]
    (let [
            sql (str 
                    "insert into " (first(:table opts) ) " (`"(first (:id opts))
                    "`, `" (first (:time opts)) "` ) values ( \"" newid
                    "\" ,\"" newtime "\") "
                )
        ]
        sql
    )
)

(defn- insert-table-new-datas [opts sqls]
    (let [mysqldb (getdatabse opts)
            _ (println sqls)
        ]
        (try
            (jdbc/with-connection mysqldb
                 (jdbc/execute! mysqldb  sqls :multi? true)
            )
            (catch SQLException e
                (println e)
                (println (.printStackTrace e) )
            )
        )        
    )
)

(defn -main [& args]
    (let [arg-spec 
            {
                :usage "Usage: dburl dbuser dbpwd table"
                :args [
                    (arg/opt :help
                        "-h|--help" "show this help"
                    )
                    (arg/opt :dburl
                        "-dburl <dburl>" "the dburl "
                    )
                    (arg/opt :dbuser
                        "-dbuser <dbuser>" 
                        "the dbuser "
                    )
                    (arg/opt :dbpwd
                        "-dbpwd <dbpwd>" 
                        "the dbpwd "
                    )
                    (arg/opt :table
                        "-table <table>" 
                        "the table "
                    )
                    (arg/opt :id
                        "-id <id>"
                        "id colum"
                    )
                    (arg/opt :time
                        "-time <time>"
                        "time colum"
                    )
                    (arg/opt :otherKey
                        "-otherKey <otherKey>"
                        "otherKeys "
                    )
                    (arg/opt :otherValue
                        "-otherValue <otherValue>"
                        "otherValue "
                    )
                    (arg/opt :num
                        "-num <num>"
                        "insert num "
                    )                 
                ]
            }
            opts (arg/transform->map (arg/parse arg-spec args))
            default-args 
                {
                    :dbsetting ["dbsetting.json"]
                    :agentsetting ["agentsetting.json"]
                    :webport ["8082"]
                }
            opts-with-default (merge default-args opts)

        ]
        (when (:help opts-with-default)
            (println (arg/default-doc arg-spec))
            (System/exit 0)            
        )
        (let  [res (first (get-table-max-data opts))
                _ (println res)
                curid ( (keyword (first (:id opts))) res)
                curtime ( (keyword(first (:time opts))) res)
                _ (println [ curid  curtime])
                dataList (iterate 
                    #(vector (+ 1 (first %)) (+ 100 (last %)) ) 
                    [ curid curtime]
                )
                usedlist (take (read-string (first (:num opts))) dataList)
                sql? (sql-gen-new-data? opts)
            ]
            (->> usedlist
                (map #(vector (str (first %)) (str (last %))))
                ;(map #(sql-gen-new-data opts (first %) (last %) ))
                (partition-all 100)
                ;(map 
                ;    (fn [sqllist] (reduce #(str %1 "\n" %2) sqllist))
                ;)
                (map #(insert-table-new-datas opts (concat [sql?] %)))
                println
            )
        )

    )
)