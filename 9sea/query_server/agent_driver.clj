(ns query-server.agent-driver
    (:require 
        [clj-http.client :as client]
    )
)

(def ^:private urlmap
    {
        :get-setting ""
        :get-schema ""
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
            tablelist (map)
        ]
    )
)

(defn new-agent [agentid, agentname,agenturl,accountid]
    (let [setting (httpget agenturl :get-setting)
            hashcode (:hashcode setting)
            appname (:app setting)
            appversion (:appversion setting)
            schema (httpget agenturl :get-schema)
            dblist (flatten (map 
                    (partial gen-table-list accountid appname appversion) 
                    schema 
                ))
        ]
    )
)




