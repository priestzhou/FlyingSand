(ns query-server.hive-adapt
    (:require
        [clojure.string :as cs]
        [query-server.core :as qc]
    )
    (:use
        [clojure.set]
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

(def ^:parivate partStr "fs_agent")

(def ^:parivate splitStr "\t")

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

(defn create-table [tn collist]
    (let [coll (map  get-column collist)
            colsql (reduce #(str %1 "," %2) coll)
            mainSql (str 
                " CREATE TABLE " tn 
                " ( " colsql 
                ") PARTITIONED BY ("partStr" STRING) "
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY \"\\1\"" 
                )
            res (qc/run-shark-query' "" mainSql)
        ]
        res
    )
)

(defn check-partition [tn pn]
    (let [mainSql (str "SHOW PARTITIONS " tn " PARTITION (" partStr "=\"" pn "\")")
            res (qc/run-shark-query' "" mainSql)
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
            res (qc/run-shark-query' "" mainSql)
        ]
        res
    )
)

(defn get-partition-location [tn pn]
    (let [mainSql (str "show table extended like " 
                    tn " PARTITION (" partStr "=\"" pn "\")"
                )
            res (qc/run-shark-query' "" mainSql)
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
            res (qc/run-shark-query' "" mainSql)
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
            res (qc/run-shark-query' "" mainSql)
        ]
      (prn "get-hive-cols" res)
      (map #(select-keys % [:col_name :data_type]) res)
    )
)

(defn get-table-schema
  [schema]
  (let [hive-table (get schema :hive_name)
        table-name (get schema :TableName)
        cols (transform-cols (get-hive-cols hive-table))]
    (prn "table column" cols)
    {
     :type "table"
     :name table-name
     :hive-name hive-table
     :children (into [] cols)
    }
  )
)
