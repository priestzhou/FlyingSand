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
            (POST "/login" {params :params}
                (prn params)
                {:status 201
                    :headers {
                        "content-type" "text/html"
                    }
                    :cookies {"email" (:email params)}
                    :body "
<!doctype html>
<html>
<head>
<meta http-equiv='refresh' content='1;url=/hello'>
</head>
</html>
"
                }
            )
            (GET "/hello" {:keys [cookies]} 
                (prn cookies)
                (format "Hello, %s" (-> cookies (get "email") (:value)))
            )
            (route/files "/" {:root (:dir opts) :allow-symlinks? true})
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
