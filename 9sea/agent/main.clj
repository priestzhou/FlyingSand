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
    )
    (:gen-class)
)

[]

(def ^:private dbatom
    (atom {})
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
        (println "t1")
        (let [dbsetting (->>
                            opts-with-default
                            :dbsetting
                            first
                            slurp
                            (#(js/read-str % :key-fn keyword))
                )
                t1 (println "==" dbsetting)
            ]
            (println dbsetting)
            (reset! dbatom dbsetting)
        )
        (println "t2")
        (rj/run-jetty #'app 
            {
                :port 
                (read-string (first (:webport opts-with-default))) 
                :join? false
            }
        )    
    )
)
