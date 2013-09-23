(ns query-server.web-server
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST)]
        [ring.adapter.jetty :only (run-jetty)]
        [logging.core :only [defloggers]]
    )
)

(defloggers debug info warn error)

(defn app [opts]
    (handler/site
        (defroutes app-routes
            (GET "/test" {params :params} 
                (format "You requested with query %s" params)
            )
            (POST "/query/create" {params :params}
                {:status 202
                    :headers {
                        "Access-Control-Allow-Origin" "*"
                        "content-type" "application/json"
                    }
                }
            )
            (route/files "/" {:root (:dir opts)})
            (route/not-found "Not Found")
        )
    )
)

(defn start [opts]
    (run-jetty (app opts)
        {
            :port (:port opts)
            :join? true
        }
    )
)
