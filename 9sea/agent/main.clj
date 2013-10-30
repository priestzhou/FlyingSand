(ns agent.main
    (:use 
        [ring.middleware.params :only (wrap-params)]
        [logging.core :only [defloggers]]
        [utilities.core :only (except->str)]
    )    
    (:require
        [argparser.core :as arg]
        [ring.adapter.jetty :as rj]
        [ring.util.response :as rp]
        [compojure.core :as cp]        
        [compojure.handler :as handler]
        [compojure.route :as route]
        [clojure.data.json :as js]
        [agent.dbadapt :as dba]
        [agent.mysqladapt :as mysql]
        [monitor.tools :as tool]
        [utilities.aes :as aes] 
    )
    (:gen-class)
)

(defloggers debug info warn error)

(def ^:private dbatom
    (atom {})
)

(defn- set-parse [smap hc]
    (let [db1 (:databse smap)
            db2 (map #(dissoc % :dbuser :dbpassword) db1)
        ]
        (assoc smap :databse db2 :hashcode hc)
    )
)

(defn- encryptWrt [s]
    (-> s
        (js/write-str)
        (aes/encrypt "fs_agent_enrypt_key_1")
    )
)

(cp/defroutes app-routes
    (cp/GET "/setting/list" {params :params} 
        (info "/setting/list")
        (if (map? @dbatom)
            (let [h (hash @dbatom )]
                {:status 200
                    :headers {
                        "Access-Control-Allow-Origin" "*"
                        "Content-Type" "application/json"
                    }
                    :body (encryptWrt (set-parse @dbatom h) )
                }
            )
            {:status 503
                    :headers {
                        "Access-Control-Allow-Origin" "*"
                        "Content-Type" "application/json"
                    }
                :body (encryptWrt 
                        {
                            :errCode 1001
                            :errStr @dbatom
                        } 
                    )
            }            
        )
    )
    (cp/GET "/schemas/all" {params :params}
        (info "into get schemas" )
        (let [r (dba/get-schemas @dbatom)]
            (debug "schemas result count" (count r))
            {:status 200
                :headers {
                    "Access-Control-Allow-Origin" "*"
                    "Content-Type" "application/json"
                }
                :body (encryptWrt r)
            }
        )
    )
    (cp/GET "/data/get/all" {params :params}
        (info "into get all data" (js/write-str params))
        (let [r (dba/get-table-all-data @dbatom 
                     (:dbname params) (:tablename params) 
                )
            ]
            (debug "all data result count " (count r))
            {:status 200
                :headers {
                    "Access-Control-Allow-Origin" "*"
                    "Content-Type" "application/json ; charset=UTF-8"
                }
                :body (encryptWrt r)
            }
        )
    )
    (cp/GET "/data/get/inc" {params :params}
        (info "into get inc data" (js/write-str params))
        (let [r (dba/get-table-inc-data @dbatom 
                    (:dbname params) (:tablename params) (:keynum params)
                )
            ]
            ;(println r)
            (debug "all inc result count" (count r) )
                {:status 200
                    :headers {
                        "Access-Control-Allow-Origin" "*"
                        "Content-Type" "application/json ; charset=UTF-8"
                    }
                    :body (encryptWrt r )
                }  
        )
    )    
    (route/not-found "Not Found")
)

(def ^:private app
    (handler/site app-routes)
)

(defn -main [& args]
    (let [arg-spec 
            {
                :usage "Usage: [dbsetting] [agentsetting] [webport]"
                :args [
                    (arg/opt :help
                        "-h|--help" "show this help"
                    )
                    (arg/opt :dbsetting
                        "-dbsetting <dbsetting>" "the dbsetting json file"
                    )
                    (arg/opt :agentsetting
                        "-agentsetting <agentsetting>" 
                        "the agentsetting json file"
                    )
                    (arg/opt :webport
                        "-webport <webport>" 
                        "the agent webport"
                    )                    
                ]
            }
            opts (arg/transform->map (arg/parse arg-spec args))
            default-args 
                {
                    :dbsetting ["dbsetting.json"]
                    :agentsetting ["agentsetting.json"]
                    :webport ["8082"]
                }
            opts-with-default (merge default-args opts)

        ]
        (when (:help opts-with-default)
            (println (arg/default-doc arg-spec))
            (System/exit 0)            
        )
        (try 
            (let [dbsetting (->
                            opts-with-default
                            :dbsetting
                            first
                            slurp
                            (js/read-str :key-fn keyword)
                    )
                ]
                (reset! dbatom dbsetting)
            )
            (catch Exception e
                (error (except->str e))
                (reset! dbatom (str e))
            )
        )
        (rj/run-jetty #'app 
            {
                :port 
                (read-string (first (:webport opts-with-default))) 
                :join? false
            }
        )
        (future 
            (tool/check 
                "\" monitor.main\""  
                "./start_monitor.sh"  
                5000
            )
        )
    )
)
;" java -cp  .:monitor.jar monitor.mian "