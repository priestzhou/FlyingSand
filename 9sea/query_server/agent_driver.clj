(ns query-server.agent-driver
    (:require 
        [org.httpkit.client :as client]
        [query-server.query-backend :as qb]
        [query-server.hive-adapt :as ha]
        [clojure.java.jdbc :as jdbc]
        [clojure.data.json :as js]
        [utilities.core :as uc]
        [argparser.core :as arg]
        [hdfs.core :as hc]
        [query-server.mysql-connector :as mysql]
    )
    (:gen-class)
)

(defn- get-group-time [timeStep timeValue]
    (let [modTime (mod timeValue timeStep)
            groupTime (- timeValue modTime)
        ]
        (.format 
            (java.text.SimpleDateFormat. "yyyy-MM-dd-HH_mm_ss") 
            groupTime
        )
    )
)

(def ^:private urlmap
    {
        :get-setting "/get-setting"
        :get-schema "/get-schemas"
        :get-table-inc-data "/get-table-inc-data"
        :get-table-all-data "/get-table-all-data"
    }
)

(defn- runsql [sql]
                (jdbc/with-connection (mysql/get-mysql-db)
                    (jdbc/with-query-results res [sql]
                        (->>
                            res
                            doall
                        )
                    )
                )
)

(defn- runupdate [sql]
                (jdbc/with-connection (mysql/get-mysql-db)
                    (jdbc/do-commands sql)
                )
)

(defn- get-hash [type data]
    (-> type
        (java.security.MessageDigest/getInstance)
        (.digest data)
    )
)
 
(defn sha1-hash [data]
    (uc/hexdigits 
        (get-hash "sha1" 
            (uc/str->bytes data) 
        )
    )
)

(def ^:private agent-stat-map
    {
        "new" 1
        "normal" 2
        "delete" 9
    }
)

(defn- get-agent-list' [sstr]
    (let [sql (str "select * from TblAgent where agentState='" sstr "'")]
        (runsql sql)
    )
)

(defn chage-agent-stat [id state]
    (let [sstr (get agent-stat-map state)
            sql (str "update TblAgent set agentState='" sstr "'"
                    " where id ='" id "'"
                )
        ]
        (runupdate sql)
    )
)

(defn get-agent-list [state]
    (get-agent-list' (get agent-stat-map state))
)

(defn- check-table [tns]
    (let [sql (str "select * from TblMetaStore where namespace = \"" tns "\"")
            res (runsql sql)
            rcount (count   res)
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
        (println  "httpget" (str agenturl (op urlmap) params))
        @(client/get (str agenturl (op urlmap) params))
    )
    ([agenturl op]
        (println  "httpget" (str agenturl (op urlmap) ))
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
    (let [collist (reduce  #(str  %1 "\",\"" %2)  allcol )
            colstr (str "( \"" collist "\")")
            sql (str  "insert into " table " VALUES " colstr ";"
            )
            res (runupdate sql)
        ]
        res
    )
)

(defn check-agent-table [agentid tns]
    (let [sql (str 
                "select * from TblSchema where namespace = \"" 
                tns "\" and agentid ='" agentid "'"
            )
            res (runsql sql)
            rcount (count   res)
        ]
        (cond
            (nil? res) true
            (> 1 rcount) true
            :else false
        )
    )
)

(defn create-table [agentid appname appversion dataset]
    (println agentid "-" appname "-" appversion "-" dataset)
    (when (check-agent-table agentid (:namespace dataset))
        (add-record "TblSchema"
            (:namespace dataset)
            agentid
            (if (= "true" (:hasTimestamp (:tableset dataset)))
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
    (println "new-agent" agentid "-" agentname "-" agenturl "-" accountid)
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
            _ (println "datalist" datalist)
        ]
        (doall 
            (map (partial create-table agentid appname appversion ) datalist )
        )
    )
)

(defn- query-agent-schema [agentid]
    (let [sql (str "select *  from TblSchema a  left join TblMetaStore b " 
            " on a.Namespace = b.Namespace   where agentid ='" agentid "'")
            res (runsql sql)
        ]
        res
    )
)

(defn- get-max-key [inlist tskey]
    (if (empty? inlist)
        nil
        (let [keylist (map #(get % tskey ) inlist)]
            (apply max keylist)
        )
    )
)

(defn- inc-data-filter [inlist tskey]
    (if (empty? inlist)
        inlist
        (let [maxkey (get-max-key inlist tskey)]
            (filter #(not= maxkey (get % tskey)) inlist)
        )
    )
)

(defn- save-inc-data [location inlist metalist]
    (let [ts (System/currentTimeMillis)
            filepath (str location "/" (get-group-time 3600000 ts) ".txt")
            filecontext (if (hc/exists? filepath)
                (hc/read-lines filepath)
                []
            )
            strList (map #(ha/build-hive-txt-row % metalist) inlist)
        ]
        (dorun (hc/write-lines filepath  (concat filecontext strList) ))
    )
)

(defn- updata-inc-key [maxkey tns agentid]
    (let [sql (str "update TblSchema set TimestampPosition=\"" maxkey "\""
                " where NameSpace=\"" tns "\" and AgentID =\"" agentid "\"" 
            )
            res (runupdate sql)
        ]
        res
    )
)

(defn- get-inc-data [agentid agenturl tableinfo]
    (println agentid "-" agenturl "-" tableinfo)
    (let [dbname (:dbname tableinfo)
            tablename (:tablename tableinfo)
            position (:timestampposition tableinfo)
            res (httpget 
                    agenturl 
                    :get-table-inc-data 
                    (str 
                        "?dbname=" dbname 
                        "&tablename=" tablename 
                        "&keynum=" position
                    )
                )
            resList (get (js/read-str (:body res)) "data")
            hiveName (:hive_name tableinfo)
            tskey (:timestampkey tableinfo)
            filterList (inc-data-filter resList tskey)
            maxkey (get-max-key filterList tskey)
        ]
        (when (not (ha/check-partition hiveName agentid))
            (ha/add-partition hiveName agentid)
        )
        (when (not (empty? filterList))
            (save-inc-data (ha/get-partition-location hiveName agentid) 
                filterList
                (ha/get-hive-clos hiveName)
            )            
            (updata-inc-key
                maxkey
                (:namespace tableinfo)
                agentid
            )
        )
    )
)

(defn- save-all-data [location inlist metalist]
    (let [ts (System/currentTimeMillis)
            filepath (str location "/" ts ".txt")
            strList (map #(ha/build-hive-txt-row % metalist) inlist)
        ]
        (hc/delete (str location "/*"))
        (dorun (hc/write-lines filepath  strList))
    )
)

(defn- get-all-data [agentid agenturl tableinfo]
    (println agentid "-" agenturl "-" tableinfo)
    (let [dbname (:dbname tableinfo)
            tablename (:tablename tableinfo)
            position (:timestampposition tableinfo)
            res (httpget 
                    agenturl 
                    :get-table-all-data 
                    (str 
                        "?dbname=" dbname 
                        "&tablename=" tablename 
                    )
                )
            resList (get (js/read-str (:body res)) "data")
            hiveName (:hive_name tableinfo)
            
        ]
        (when (not (ha/check-partition hiveName agentid))
            (ha/add-partition hiveName agentid)
        )
        (save-all-data (ha/get-partition-location hiveName agentid) 
            resList
            (ha/get-hive-clos hiveName)
        )
    )
)

(defn- get-table-data-both [agentid agenturl tableinfo]
    (println "both=" agentid "-" agenturl "-" tableinfo)
    (if (:hastimestamp tableinfo)
        (get-inc-data agentid,agenturl tableinfo)
        (get-all-data agentid,agenturl tableinfo)
    )
)

(defn- get-table-data-inc [agentid agenturl tableinfo]
    (println "inc=" agentid "-" agenturl "-" tableinfo)
    (when (:hastimestamp tableinfo)
        (get-inc-data agentid,agenturl tableinfo)
    )
)

(defn- get-table-data-all [agentid agenturl tableinfo]
    (println "all=" agentid "-" agenturl "-" tableinfo)
    (when (not (:hastimestamp tableinfo))
        (get-all-data agentid,agenturl tableinfo)
    )
)

(defn get-agent-data [agentid agenturl type]
    (println "get-agent-data=" agentid "-" agenturl "-" type)    
    (let [tablelist (query-agent-schema agentid)]
            (when (= type "both")
                (doall
                    (map (partial get-table-data-both agentid, agenturl) tablelist)
                )
            )
            (when (= type "inc")
                (doall
                    (map (partial get-table-data-inc agentid, agenturl) tablelist)
                )
            )
            (when (= type "all")
                (doall
                    (map (partial get-table-data-all agentid, agenturl) tablelist)
                )
            )        
    )
)

(defn -main [& args]
    (let [arg-spec 
            {
                :usage "Usage: [new-agent] [get-agent-data] [temp]"
                :args [
                    (arg/opt :help
                        "-h|--help" "show this help"
                    )
                    (arg/opt :new-agent
                        "-new-agent " "run new-agent test`"
                    )
                    (arg/opt :get-agent-data
                        "-get-agent-data " 
                        "run get-agent-data test"
                    )
                    (arg/opt :temp
                        "-temp " 
                        "run get-agent-data test"
                    )
                ]
            }
            opts (arg/transform->map (arg/parse arg-spec args))
        ]
        (when (:help opts)
            (println (arg/default-doc arg-spec))
            (System/exit 0)            
        )
        (when (:new-agent opts)
            (println (new-agent "11" "dfdsf" "http://192.168.1.101:8082" "user1"))
        )
        (when (:get-agent-data opts)
            (println (get-agent-data "11" "http://192.168.1.101:8082"))        
        )
        (when (:temp opts)
            (println 
                (ha/get-hive-clos 
                    "tn_58c0234ac5849eb286c51f648aaf6c47a6122c38"
                    ;"fs_agent=\"test1\""
                )
            )
        )
    )
)
