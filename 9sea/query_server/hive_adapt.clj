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
        (re-find #"[a-zA-Z]+")
        (cs/upper-case)
        (get mysql-hive-type-map)
    )
)

(defn- get-column [colMap]
    (let [colname (:Field colMap)
            colType (:Type colMap)
            hiveType (mysql-type-to-hive colType)
        ]
        (str colname " " hiveType ",")
    )
)

(defn create-table [tn collist]
    (let [ colsql (apply str (map  get-column collist))
            mainSql (str 
                " CREATE TABLE " tn 
                " (" colsql 
                ") PARTITIONED BY (fs_agent STRING)" 
                )
        ]
        (qc/run-shark-query "" mainSql)
    )
)
