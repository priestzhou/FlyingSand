(ns query-server.hive-adapt
    (:require
        [clojure.string :as cs]
        [query-server.core :as qc]
    )
)

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
        (str colname " " hiveType )
    )
)

(defn create-table [tn collist]
    (let [ colsql (reduce #(str %1 "," %2) (map  get-column collist))
            mainSql (str 
                " CREATE TABLE " tn 
                " ( " colsql 
                ") PARTITIONED BY (fs_agent STRING) " 
                )
            t1 (print "hive sql " mainSql)
            res (qc/run-shark-query "" mainSql)
        ]
        res
    )
)
