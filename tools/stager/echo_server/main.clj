(ns echo-server.main
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
        [clojure.string :as str]
        [clojure.java.io :as io]
        [org.httpkit.server :as http]
        [clojure.pprint :as pp]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST DELETE HEAD ANY)]
        [slingshot.slingshot :only (try+ throw+)]
    )
    (:gen-class)
)

(defn app [opts]
    (handler/api
        (defroutes app-routes
            (GET "/:path" [path]
                {
                    :status 200
                    :headers {"Content-Type" "text/plain"}
                    :body path
                }
            )
            (route/not-found "Not Found")
        )
    )
)

(defn -main [& args]
    (http/run-server (app {}) {:port 12345})
)

