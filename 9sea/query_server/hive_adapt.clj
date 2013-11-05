(ns query-server.hive-adapt
    (:require
        [clojure.string :as cs]
        [query-server.mysql-connector :as mysql]
        [clojure.java.jdbc :as sql]
        [utilities.core :as util]
    )
    (:use
        [clojure.set]
        [logging.core :only [defloggers]]
    )
)

(defloggers debug info warn error)

(def ^:private mysql-hive-type-map 
    {
        "TINYINT" "TINYINT"
        "SMALLINT" "SMALLINT"
        "MEDIUMINT" "INT"
        "INT" "INT"
        "BIGINT" "BIGINT"
        "FLOAT" "FLOAT"
        "DOUBLE" "DOUBLE"
        "CHAR" "STRING"
        "VARCHAR" "STRING"
        "TEXT" "STRING"
        "BOOLEAN" "BOOLEAN"
        "BLOB" "BINARY"
    }
)

(def ^:private partStr "fs_agent")

(def ^:private hive-db (ref {:classname "org.apache.hadoop.hive.jdbc.HiveDriver"
                             :subprotocol "hive"
                             :user ""
                             :password ""
                            }
                       )
)

(def ^:private hive-conn-str (ref ""))

<<<<<<< HEAD
(def ^:private table-type ["" "table" "ctas" "view"])

=======
>>>>>>> add view/ctas
(defn set-hive-db
  [host port]
  (dosync
    (alter hive-db conj {:subname (format "//%s:%s" host port)})
    (ref-set hive-conn-str (format "jdbc:hive://%s:%s" host port))
  )
  (prn "hive-db" @hive-db)
  (prn "hive-conn-str" @hive-conn-str)
)

(defn get-hive-db
  []
  (-> @hive-db)
)

(defn get-hive-conn-str
  []
  (-> @hive-conn-str)
)

(defn- mysql-type-to-hive [colType]
    (->>
        colType
        (re-find #"[a-zA-Z]+" )
        (cs/upper-case)
        (get mysql-hive-type-map)
    )
)

(defn- get-column [colMap]
    (let [colname (:field colMap)
            colType (:type colMap)
            hiveType (mysql-type-to-hive colType)
        ]
        (str "`"colname "` " hiveType )
    )
)

(defn run-shark-query'
  [q-id query-str]
  (try
<<<<<<< HEAD
    (debug "run-shark-query" :qid q-id)
    (prn (str "query-str:" query-str))
    (sql/with-connection (get-hive-db)
      (sql/with-query-results rs [query-str]
        (doall rs)
        ))
    (catch Exception exception
      (error "run shark query:" (util/except->str exception))
      ))
)

(defn run-shark-query-with-except-throw
  [query-str]
  (info (str "query-str:" query-str))
  (sql/with-connection (get-hive-db)
    (sql/with-query-results rs [query-str]
      (doall rs))
  )
=======
   (debug "run-shark-query" :qid q-id)
    (prn (str "query-str:" query-str))
  ( sql/with-connection (get-hive-db)
    (sql/with-query-results rs [query-str]
      (doall rs)
      ))
  (catch Exception exception
    (error "run shark query:" (util/except->str exception))
    ; we should seperate exception
;    (update-result-map q-id "failed" nil exception)
    ))
>>>>>>> add view/ctas
)

(defn create-CTAS
  [table-name table-query]
  (let [hive-sql (str " CREATE TABLE " table-name
                      " AS " table-query
                 )
<<<<<<< HEAD
        rs (run-shark-query-with-except-throw hive-sql)
=======
        rs (run-shark-query' "" hive-sql)
>>>>>>> add view/ctas
       ]
    rs
  )
)

(defn create-view
  [view-name view-query]
  (let [hive-sql (str " CREATE VIEW " view-name
                      " AS " view-query
                 )
<<<<<<< HEAD
        rs (run-shark-query-with-except-throw hive-sql)
=======
        rs (run-shark-query' "" hive-sql)
>>>>>>> add view/ctas
        ]

    rs
  )
)

(defn drop-view
  [view-name]
  (let [hive-sql (str "DROP VIEW " view-name)
<<<<<<< HEAD
        rs (run-shark-query-with-except-throw hive-sql)
=======
        rs (run-shark-query' "" hive-sql)
>>>>>>> add view/ctas
       ]
    rs
  )
)

(defn drop-CTAS
  [ctas-name]
  (let [hive-sql (str "DROP TABLE " ctas-name)
<<<<<<< HEAD
        rs (run-shark-query-with-except-throw hive-sql)
=======
        rs (run-shark-query' "" hive-sql)
>>>>>>> add view/ctas
       ]
    rs
  )
)
     

(defn create-table [tn collist]
    (let [coll (map  get-column collist)
            colsql (reduce #(str %1 "," %2) coll)
            mainSql (str 
                " CREATE TABLE " tn 
                " ( " colsql 
                ") PARTITIONED BY ("partStr" STRING) "
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY \"\\1\"" 
                )
            res (run-shark-query' "" mainSql)
        ]
        res
    )
)

(defn check-partition [tn pn]
    (let [mainSql (str "SHOW PARTITIONS " tn " PARTITION (" partStr "=\"" pn "\")")
            res (run-shark-query' "" mainSql)
        ]
        (cond
            (nil? res) false
            (= 1 (count res)) true
            :else false
        )
    )
)

(defn add-partition [tn pn]
    (let [mainSql (str "alter table " tn " add PARTITION (" partStr "=\"" pn "\")")
            res (run-shark-query' "" mainSql)
        ]
        res
    )
)

(defn get-partition-location [tn pn]
    (let [mainSql (str "show table extended like " 
                    tn " PARTITION (" partStr "=\"" pn "\")"
                )
            res (run-shark-query' "" mainSql)
            flist (filter 
                    #(->>
                    (re-find #"^location:" (:tab_name %))
                    nil?
                    not
                ) 
                res
            )
            location (->>
                    flist
                    first
                    :tab_name
                    (re-find #"(?<=^location:)[\S]+") 
                )
        ]
        location
    )
)

(defn get-hive-clos [tn]
    (let [mainSql (str "DESCRIBE " tn )
            res (run-shark-query' "" mainSql)
        ]
        (->>
            res
            (map :col_name)
            (filter #(not= % partStr))
        )
    )
)

(defn build-hive-txt-row [inmap cols]
    (->>
        cols
        (map #(get inmap % ))
        (reduce #(str %1 "\1" %2) )
    )
)

(defn transform-cols
  [raw-result]

  (let [
      ;  map-result (apply hash-map raw-result)
       res (map #(rename-keys % {:col_name :name :data_type :type}) raw-result)
    ;    r (rename-keys map-result {:col_name :name :data_type :type})
       ]
    res
  )
)

(defn get-hive-cols [tn]
    (let [mainSql (str "DESCRIBE " tn )
            res (run-shark-query' "" mainSql)
        ]
;      (println "get-hive-cols" res)
      (map #(select-keys % [:col_name :data_type]) res)
    )
)

(defn get-table-schema
  [schema]
  (let [hive-table (get schema :hive_name)
        table-name (get schema :TableName)
        ns-type (:NameSpaceType schema)
        _ (prn "get-table-schema" (get table-type ns-type))
        cols (transform-cols (get-hive-cols hive-table))]
;    (println "table column" cols)
    {
     :type (get table-type ns-type)
     :name table-name
     :hive-name hive-table
     :children (into [] cols)
    }
  )
)
