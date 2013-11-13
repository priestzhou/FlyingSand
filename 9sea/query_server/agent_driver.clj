(ns query-server.agent-driver
    (:use
        [utilities.core :only (except->str)]
        [logging.core :only [defloggers]]
    )
    (:require 
        [org.httpkit.client :as client]
        [query-server.hive-adapt :as ha]
        [clojure.java.jdbc :as jdbc]
        [clojure.data.json :as js]
        [utilities.core :as uc]
        [argparser.core :as arg]
        [hdfs.core :as hc]
        [query-server.mysql-connector :as mysql]
        [utilities.aes :as aes]
        [query-server.config :as config]
        [clj-time.core :as time]
        [clj-time.format :as tformat]
        [clj-time.coerce :as coe]
    )
    (:gen-class)
)

(defloggers debug info warn error)

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
        :get-setting "/setting/list"
        :get-schema "/schemas/all"
        :get-table-inc-data "/data/get/inc"
        :get-table-all-data "/data/get/all"
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
        "stop" 3
        "mismatch" 8
        "delete" 9
    }
)

(defn- get-agent-list' [sstr]
    (let [sql (str "select * from TblAgent where agentState='" sstr "'")]
        (runsql sql)
    )
)

(defn change-agent-stat [id state]
    (let [sstr (get agent-stat-map state)
            sql (str "update TblAgent set agentState='" sstr "'"
                    " where id ='" id "'"
                )
        ]
        (runupdate sql)
    )
)

(defn change-agent-config [id appname version confighash]
    (let [
            sql (str "update TblAgent set AppName='" appname "',"
                    " AppVersion='" version "',ConfigHash='"confighash
                    "' where id ='" id "'"
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

(defn- get-decrypt-body [res]
    (let [body (:body res)
            detxt (aes/decrypt body "fs_agent_enrypt_key_1")
        ]
        detxt
    )
)

(defn- httpget 
    ([agenturl op params]
        (info  "httpget" (str agenturl (op urlmap) params))
        @(client/get (str agenturl (op urlmap) params))
    )
    ([agenturl op]
        (info  "httpget" (str agenturl (op urlmap) ))
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

(defn- add-record-bycol [table colnames & allcol] 
    (let [collist (reduce  #(str  %1 "\",\"" %2)  allcol )
            colstr (str "( \"" collist "\")")
            sql (str  "insert into " table " (" colnames ") VALUES " colstr ";"
            )
            _ (debug "add-record-bycol" :sql sql)
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
    (debug 
        "create-table" 
        :agentid agentid
        :appname appname 
        :appversion appversion  
        :dataset (str dataset)
    )
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
            (ha/create-table (:hiveName dataset) (:cols dataset))
            (add-record "TblMetaStore"
                (:namespace dataset)
                appname 
                appversion 
                (:dbname dataset)
                (:tablename dataset)
                (:hiveName dataset)
                "1"
            )
        )
    )
)

(defn new-agent [agentid, agentname,agenturl,accountid]
    (info 
        "new-agent" 
        :agentid agentid
        :agentname agentname 
        :agenturl agenturl  
        :accountid accountid
    )
        (let [setting (js/read-str 
                    (get-decrypt-body (httpget agenturl :get-setting))
                    :key-fn keyword
                )
                hashcode (:hashcode setting)
                appname (:app setting)
                appversion (:appversion setting)
                schema (js/read-str 
                    (get-decrypt-body (httpget agenturl :get-schema)) 
                    :key-fn keyword
                )
                datalist (flatten 
                    (map 
                        #(gen-table-list accountid appname appversion %) 
                        schema 
                    )
                )
            ]
            (try 
                (add-record-bycol 
                    "TblApplication" "ApplicationName,AccountId" 
                    appname accountid
                )
                (catch Exception e 
                    (debug " add TblApplication fail " :except (except->str e))
                )
            )
            (change-agent-config agentid appname appversion hashcode)
            (doall 
                (map 
                    (partial create-table agentid appname appversion) 
                    datalist 
                )
            )
        )
)

(defn- query-agent-schema [agentid]
    (let [sql (str "select *  from TblAgent c left join TblSchema a on c.id=a.agentid 
                left join TblMetaStore b " 
                " on c.AppName=b.AppName and c.AppVersion=b.AppVersion and 
                a.Namespace = b.Namespace   where agentid ='" agentid "'"
                " and a.Namespace is not null and b.Namespace is not null")
;add app version in agent
            res (runsql sql)
        ]
        (debug "query-agent-schema" :res (str res) )
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
            filepath (str 
                    location "/" 
                    (get-group-time 
                        (config/get-key :inc-data-group-time) 
                        ts
                    ) 
                    ".txt"
                )
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

(defn- updata-agent-sync-time [agentid]
    (let [now (tformat/unparse (tformat/formatters :date-hour-minute-second) (time/now))
            _ (debug "updatetime" now)
            sql (str "update TblAgent set LastSyncTime=\"" now "\" "
                    " where id =" agentid
                )
            res (runupdate sql)
        ]
        res
    )
)

(defn- get-inc-data' [res agentid agentname tableinfo]
    (let [
            hiveName (:hive_name tableinfo)
            tskey (:timestampkey tableinfo)            
            resList (get (js/read-str (get-decrypt-body res)) "data")
            filterList (inc-data-filter resList tskey)
            maxkey (get-max-key filterList tskey)
        ]
        (when (not (ha/check-partition hiveName agentname))
            (ha/add-partition hiveName agentname)
        )
        (when (not (empty? filterList))
            (save-inc-data (ha/get-partition-location hiveName agentname) 
                filterList
                (ha/get-hive-clos hiveName)
            )            
            (updata-inc-key
                maxkey
                (:namespace tableinfo)
                agentid
            )
            (updata-agent-sync-time agentid )
        )
    )
)

(defn- get-inc-data [agentid agentname agenturl tableinfo]
    (info 
        "get-inc-data" 
        :agentid agentid 
        :agenturl agenturl  
        :tableinfo (str tableinfo)
    )
    (let [dbname (:dbname tableinfo)
            tablename (:tablename tableinfo)
            position (:timestampposition tableinfo)
            configHash(:confighash tableinfo)
            res (httpget 
                    agenturl 
                    :get-table-inc-data 
                    (str 
                        "?dbname=" dbname 
                        "&tablename=" tablename 
                        "&keynum=" position
                        "&confighash=" configHash
                    )
                )
            status (:status res)
        ]
        (cond 
            (= 200 status) 
            (do 
                (updata-agent-sync-time agentid )
                (get-inc-data' res agentid agentname tableinfo)
            )
            (and 
                (= 503 status)
                (= 2001 (get (js/read-str (get-decrypt-body res)) "errCode"))
            )
            (do
                (warn "The agent is mismatched" 
                    :agentname agentname
                )
                (change-agent-stat agentid "mismatch")
                (throw (Exception. "the agent mismatched"))
            )
            :else 
            (error 
                "The http response's status in get-inc-data is not 200" 
                :agenturl agenturl
                :tableinfo tableinfo
                :response res
            )
        )
    )
)

(defn- save-all-data [location inlist metalist]
    (let [ts (System/currentTimeMillis)
            filepath (str location "/all-data.txt")
            strList (map #(ha/build-hive-txt-row % metalist) inlist)
        ]
        (debug 
            "save-all-data" 
            :location location 
            :filepath filepath 
            :strListcount (count strList)
        )
        (hc/delete (str location "/*"))
        (dorun (hc/write-lines filepath  strList))
    )
)

(defn- get-all-data' [res agentname tableinfo]
    (let [
            resList (get (js/read-str (get-decrypt-body res)) "data")
            hiveName (:hive_name tableinfo)
            
        ]
        (when (not (ha/check-partition hiveName agentname))
            (ha/add-partition hiveName agentname)
        )
        (save-all-data (ha/get-partition-location hiveName agentname) 
            resList
            (ha/get-hive-clos hiveName)
        )
    )
)

(defn- get-all-data [agentid agentname agenturl tableinfo]
    (info "get-all-data" 
        :agentid  agentid 
        :agenturl agenturl 
        :tableinfo (str tableinfo)
    )
    (let [dbname (:dbname tableinfo)
            tablename (:tablename tableinfo)
            position (:timestampposition tableinfo)
            configHash(:confighash tableinfo)
            res (httpget 
                    agenturl 
                    :get-table-all-data 
                    (str 
                        "?dbname=" dbname 
                        "&tablename=" tablename 
                        "&confighash=" configHash
                    )
                )
            status (:status res)
        ]
        (cond 
            (= 200 status) 
            (do
                (get-all-data' res agentname tableinfo)
                (updata-agent-sync-time agentid )
            )
            (and 
                (= 503 status)
                (= 2001 (get (js/read-str (get-decrypt-body res)) "errCode"))
            )
            (do
                (warn "the agent mismatched" 
                    :agentname agentname
                )
                (change-agent-stat agentid "mismatch")
                (throw (Exception. "the agent mismatched"))
            )            
            :else 
            (error "The http response's status is not 200" 
                :agenturl agenturl
            )
        )
    )
)

(defn- get-table-data-both [agentid agentname agenturl tableinfo]
    (info "get-table-data-both" 
        :agentid  agentid 
        :agenturl agenturl 
        :tableinfo (str tableinfo)
    )
    (if (:hastimestamp tableinfo)
        (get-inc-data agentid agentname agenturl tableinfo)
        (get-all-data agentid agentname agenturl tableinfo)
    )
)

(defn- get-table-data-inc [agentid agentname agenturl tableinfo]
    (info "get-table-data-inc" 
        :agentid  agentid 
        :agenturl agenturl 
        :tableinfo (str tableinfo)
    )
    (when (:hastimestamp tableinfo)
        (get-inc-data agentid agentname agenturl tableinfo)
    )
)

(defn- get-table-data-all [agentid agentname agenturl tableinfo]
    (info "get-table-data-all" 
        :agentid  agentid 
        :agenturl agenturl 
        :tableinfo (str tableinfo)
    )
    (when (not (:hastimestamp tableinfo))
        (get-all-data agentid agentname agenturl tableinfo)
    )
)

(defn get-agent-data [agentid agentname agenturl type]
    (info "get-agent-data" 
        :agentid  agentid 
        :agenturl agenturl 
        :type type
    )
    (try
        (let [tablelist (query-agent-schema agentid)]
            (when (= type "both")
                (doall
                    (map (partial get-table-data-both agentid agentname agenturl) tablelist)
                )
            )
            (when (= type "inc")
                (doall
                    (map (partial get-table-data-inc agentid agentname agenturl) tablelist)
                )
            )
            (when (= type "all")
                (doall
                    (map (partial get-table-data-all agentid agentname agenturl) tablelist)
                )
            )        
        )
        (catch Exception e
            (error  (except->str e))
        )
    )
)

(defn check-mismatch-agent [agentid appname appversion agentname agenturl 
    accountid]
    (let [setting (js/read-str 
                    (get-decrypt-body (httpget agenturl :get-setting))
                    :key-fn keyword
                )
            newhashcode (:hashcode setting)
            newappname (:app setting)
            newappversion (:appversion setting)
        ]
        (new-agent agentid agentname agenturl accountid)
        (debug "the agent renewed" :agentname agentname)
        (change-agent-stat agentid "normal")
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
