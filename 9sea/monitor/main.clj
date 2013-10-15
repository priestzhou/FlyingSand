(ns monitor.main
    (:use 
        [ring.middleware.params :only (wrap-params)]
    )    
    (:require
        [argparser.core :as arg]
        [ring.adapter.jetty :as rj]
        [ring.util.response :as rp]
        [compojure.core :as cp]        
        [compojure.handler :as handler]
        [compojure.route :as route]
        [monitor.tools :as tool] 
    )
    (:gen-class)
)

(cp/defroutes app-routes
    (cp/GET "/file/list" {params :params} 
        (if (nil? (:path params))
            (let [r (tool/fileList "./")]
                {:status 202
                    :headers {
                        "Access-Control-Allow-Origin" "*"
                        "Content-Type" "application/json"
                    }
                    :body   r 
                }
            )
            (let [r (tool/fileList (:path params))]
                {:status 202
                    :headers {
                        "Access-Control-Allow-Origin" "*"
                        "Content-Type" "application/json"
                    }
                    :body   r 
                }
            )            
        )

         
        
    )
    (cp/GET "/file/down" {params :params}
            {:status 202
                    :headers {
                        "Access-Control-Allow-Origin" "*"
                        "Content-Type" "application/json"
                    }
                    :body  (str "sdfsdf") 
                }
    )
    (route/not-found "Not Found")
)

(def ^:private app
    (handler/site app-routes)
)

(defn now [] (new java.util.Date))

(defn -main [& args]
    (println "start monitor" (now))
    (comment future 
        (tool/check 
            "agent.main" 
            " nohup java -cp .:agent.jar agent.main 2>&1 >>agent.log & " 
            5000
        )
    )
    (let [arg-spec 
            {
                :usage "Usage: [webport]"
                :args [
                    (arg/opt :help
                        "-h|--help" "show this help"
                    )
                    (arg/opt :webport
                        "-webport <webport>" 
                        "the monitor webport"
                    )                    
                ]
            }
            opts (arg/transform->map (arg/parse arg-spec args))
            default-args 
                {
                    :webport ["8081"]
                }
            opts-with-default (merge default-args opts)

        ]
        (when (:help opts-with-default)
            (println (arg/default-doc arg-spec))
            (System/exit 0)            
        )
        (rj/run-jetty #'app 
            {
                :port 
                (read-string (first (:webport opts-with-default))) 
                :join? false
            }
        )
    )    
)

