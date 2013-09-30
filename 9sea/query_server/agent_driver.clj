(ns query-server.agent-driver
    (:require 
        [clj-http.client :as client]
        [query-server.query-backend :as qb]
        [query-server.hive-adapt :as ha]
    )
)

(def ^:private urlmap
    {
        :get-setting "/get-setting"
        :get-schema "/get-schemas"
    }
)

(defn- httpget 
    ([agenturl op params]
        (client/get (str agenturl (op urlmap) params))
    )
    ([agenturl op]
        (client/get (str agenturl (op urlmap) ))
    )
)

(defn- gen-table-list [accountid appname appversion dbsc]
    (let [dbname (:dbname dbsc)
            tbl (:tables dbsc)
            tns (str accountid "/" appname "/" appversion "/" dbname)
        ]
        (map #(assoc % :namespace (str tns "." (:tablename %) )) tbl)
    )
)

(defn new-agent [agentid, agentname,agenturl,accountid]
    (let [setting (httpget agenturl :get-setting)
            hashcode (:hashcode setting)
            appname (:app setting)
            appversion (:appversion setting)
            schema (httpget agenturl :get-schema)
            datalist (flatten (map 
                    (partial gen-table-list accountid appname appversion) 
                    schema 
                ))
        ]
        (map create-table datalist)
    )
)

(defn- runsql [sql]
                (jdbc/with-connection qb/my-db
                    (jdbc/with-query-results res [sql]
                        (->>
                            res
                            doall
                        )
                    )
                )
)

(defn- check-table [tns]
    (let [sql (str "select * from TblMetaStore where namespace = '" tns "'")
            rcount (count   (runsql sql))
        ]
        (cond
            (<= 1 rcount) true
            :else false
        )
    )

)

(defn create-table [dataset]
    (when (check-table (:namespace dataset))
        (ha/create-table (:namespace dataset) (:cols dataset))
    )
)

(defn -main []
    (new-agent "11" "dfdsf" "http://192.168.1.101:8082" "sdfdsf")
)



