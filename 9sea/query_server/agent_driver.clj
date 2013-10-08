(ns query-server.agent-driver
    (:require 
        [org.httpkit.client :as client]
        [query-server.query-backend :as qb]
        [query-server.hive-adapt :as ha]
        [clojure.java.jdbc :as jdbc]
        [clojure.data.json :as js]
    )
    (:gen-class)
)

(def ^:private urlmap
    {
        :get-setting "/get-setting"
        :get-schema "/get-schemas"
    }
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

(defn- runupdate [sql]
                (jdbc/with-connection qb/my-db
                    (jdbc/execute! qb/my-db  [sql])
                )
)

(defn- check-table [tns]
    (let [sql (str "select * from TblMetaStore where namespace = '" tns "'")
            res (runsql sql)
            rcount (count   res)
            t1 (println "res " res " rcount" rcount)
        ]
        (cond
            (nil? res) true
            (> 1 rcount) true
            :else false
        )
    )
)

(defn- httpget 
    ([agenturl op params]
        @(client/get (str agenturl (op urlmap) params))
    )
    ([agenturl op]
        @(client/get (str agenturl (op urlmap) ))
    )
)

(defn- gen-table-list [accountid appname appversion dbsc]
    (let [
            dbname (:dbname dbsc)
            tbl (:tables dbsc)
            tns (str accountid "." appname "." appversion "." dbname)
        ]
        (map 
            #(assoc % 
                :namespace (str tns "." (:tablename %) )
                :hiveName (str "tn_" (hash (str tns "." (:tablename %) )))
                :dbname dbname
            ) 
            tbl
        )
    )
)

(defn- add-table [& allcol] 
    (let [collist (reduce  #(str  %1 "','" %2)  allcol )
            colstr (str "('" collist "')")
            sql (str  "insert into TblMetaStore VALUES " colstr ";"
            )
            t1 (println sql)
            res (runupdate sql)
        ]
        res
    )
)

(defn create-table [appname appversion dataset]
    (println dataset)
    (when (check-table (:namespace dataset))
        (do 
            (add-table 
                (:namespace dataset) 
                appname 
                appversion 
                (:dbname dataset)
                (:tablename dataset)
                (:hiveName dataset)
            )
            (ha/create-table (:hiveName dataset) (:cols dataset))
        )
    )
)

(defn new-agent [agentid, agentname,agenturl,accountid]
    (let [setting (js/read-str 
                (:body (httpget agenturl :get-setting))
                :key-fn keyword
            )
            hashcode (:hashcode setting)
            appname (:app setting)
            appversion (:appversion setting)
            schema (js/read-str 
                    (:body (httpget agenturl :get-schema)) 
                    :key-fn keyword
                )
            datalist (flatten 
                (map 
                    #(gen-table-list accountid appname appversion %) 
                    schema 
                )
            )
        ]
        (map (partial create-table appname appversion) datalist )
    )
)

(defn -main []
    (println (new-agent "11" "dfdsf" "http://192.168.1.101:8082" "user1"))
)



