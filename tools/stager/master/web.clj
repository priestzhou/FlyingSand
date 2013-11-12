(ns master.web
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
        [clojure.string :as str]
        [clojure.java.io :as io]
        [org.httpkit.server :as http]
        [clojure.data.json :as json]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST DELETE HEAD ANY)]
        [slingshot.slingshot :only (try+ throw+)]
        [logging.core :only [defloggers]]
    )
)

(defloggers debug info warn error)

(defn- get-slaves [])
(defn- add-slave [_])

(defn- app [opts]
    (handler/api
        (defroutes app-routes
            (GET "/ruok" [] {
                :status 200
                :headers {"Content-Type" "text/plain"}
                :body "imok"
            })
            (GET "/slaves/" [] (get-slaves))
            (POST "/slaves/" {:keys [params]}
                (add-slave params)
            )
            (route/files "/" {:root (:resource-root opts) :allow-symlinks? true})
            (route/not-found "Not Found")
        )
    )
)

(defn start-server [opts]
    (http/run-server (app opts) {:port (:port opts)})
)
