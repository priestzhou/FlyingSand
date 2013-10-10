(ns query-server.agent-driver
    (:require 
        [org.httpkit.client :as client]
        [query-server.query-backend :as qb]
        [query-server.hive-adapt :as ha]
        [clojure.java.jdbc :as jdbc]
        [clojure.data.json :as js]
        [utilities.core :as uc]
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
    (println "sql" sql)
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

(defn- get-hash [type data]
    (.digest (java.security.MessageDigest/getInstance type)  data )
)
 
(defn sha1-hash [data]
    (uc/hexdigits 
        (get-hash "sha1" 
            (uc/str->bytes data) 
        )
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
            t1 (println " dbsc " dbsc)
            tbl (:tables dbsc)
            tns (str accountid "." appname "." appversion "." dbname)
        ]
        (map 
            #(assoc % 
                :namespace (str tns "." (:tablename %) )
                :hiveName (str "tn_" (sha1-hash (str tns "." (:tablename %) )))
                :dbname dbname
                :tableset (get-in 
                            dbsc 
                            [(keyword dbname) (keyword(:tablename %))]
                        )
            ) 
            tbl
        )
    )
)

(defn- add-record [table & allcol] 
    (let [collist (reduce  #(str  %1 "','" %2)  allcol )
            colstr (str "('" collist "')")
            sql (str  "insert into " table " VALUES " colstr ";"
            )
            t1 (println sql)
            res (runupdate sql)
        ]
        res
    )
)

(defn check-agent-table [agentid tns]
    (let [sql (str 
                "select * from TblSchema where namespace = '" 
                tns "' and agentid ='" agentid "'"
            )
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

(defn create-table [agentid appname appversion dataset]
    (println dataset)
    (when (check-agent-table agentid (:namespace dataset))
        (add-record "TblSchema"
            (:namespace dataset)
            agentid
            (if (:hasTimestamp (:tableset dataset))
                1
                0
            )
            0
            (:timestampCol (:tableset dataset))
        )
    )
    (when (check-table (:namespace dataset))
        (do 
            (add-record "TblMetaStore"
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
            t1 (println "datalist" datalist)
        ]
        (map (partial create-table agentid appname appversion ) datalist )
    )
)

(defn- query-agent-schema [agentid]
    (let [sql (str "select *  from TblSchema a  left join TblMetaStore b " 
            " on a.Namespace = b.Namespace   where agentid ='" agentid "'")
            res (runsql sql)
        ]
        (println res )
    )
)

(defn- get-inc-data [agentid agenturl tableinfo]
    (let [dbname (:dbname tableinfo)
            tablename (:tablename tableinfo)
            position (:timestampposition tableinfo)
            res (httpget 
                    agenturl 
                    "/get-table-inc-data" 
                    (str 
                        "?dbname=" dbname 
                        "&tablename=" tablename 
                        "&keynum=" position
                    )
                )
        ]
        (println res)
    )
)

(defn- get-table-data [agentid agenturl tableinfo]
    (println tableinfo)
    (if (:hastimestam tableinfo)
        (get-inc-data agentid,agenturl tableinfo)
    )
)

(defn get-agent-data [agentid agenturl]
    (let [tablelist (query-agent-schema agentid)]
        (map (partial get-table-data agentid, agenturl) tablelist)
    )
)

(defn -main []
    (println (new-agent "11" "dfdsf" "http://192.168.1.101:8082" "user1"))    
)

(comment println (get-agent-data "11" "http://192.168.1.101:8082"))

(comment println (new-agent "11" "dfdsf" "http://192.168.1.101:8082" "user1"))


