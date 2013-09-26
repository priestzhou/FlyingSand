(ns agent.main
    (:use 
        [ring.middleware.params :only (wrap-params)]
        [logging.core :only [defloggers]]
    )    
    (:require
        [argparser.core :as arg]
        [ring.adapter.jetty :as rj]
        [compojure.core :as cp]        
        [compojure.handler :as handler]
        [compojure.route :as route]
        [clojure.data.json :as js]
        [agent.dbadapt :as dba]
        [agent.mysqladapt :as mysql]
        [monitor.tools :as tool]        
    )
    (:gen-class)
)

[]

(def ^:private dbatom
    (atom {})
)

(def db-table-list 
    (memoize dba/get-db-table-list )
)

(cp/defroutes app-routes
    (cp/GET "/test" {params :params} 
        (dba/get-schemas @dbatom)
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
        (let [dbsetting (->>
                            opts-with-default
                            :dbsetting
                            first
                            slurp
                            (#(js/read-str % :key-fn keyword))
                )
            ]
            (reset! dbatom dbsetting)
        )
        (rj/run-jetty #'app 
            {
                :port 
                (read-string (first (:webport opts-with-default))) 
                :join? false
            }
        )
        (println  (db-table-list @dbatom)   )
        (println (get-in (db-table-list @dbatom) ["test" "test1"])  )
        (println (get-in (db-table-list @dbatom) ["test" "test"])  )
        (tool/check 
            "monitor.main"  
            " nohup java -cp  .:monitor.jar monitor.main 2>&1 >>monitor.log & "  
            10000
        )
    )
)
;" java -cp  .:monitor.jar monitor.mian "