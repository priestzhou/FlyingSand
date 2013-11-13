(ns master.web
    (:require
        [compojure.handler :as handler]
        [compojure.route :as route]
        [clojure.string :as str]
        [clojure.java.io :as io]
        [org.httpkit.server :as http]
        [clojure.data.json :as json]
        [utilities.core :as util]
        [master.app :as app]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST DELETE HEAD ANY)]
        [slingshot.slingshot :only (try+ throw+)]
        [logging.core :only [defloggers]]
    )
)

(defloggers debug info warn error)

(defn- ruok []
    {
        :status 200
        :headers {"Content-Type" "text/plain"}
        :body "imok"
    }
)

(defn- handle [h]
    (try+
        (h)
    (catch map? ex
        ex
    )
    (catch Exception ex
        (error "exception!" :error (util/except->str ex))
    )
    (catch Error ex
        (error "error!" :error (util/except->str ex))
        (System/exit 1)
    ))
)

(defn- app [opts]
    (handler/api
        (defroutes app-routes
            (GET "/ruok" [] (ruok))

            (GET "/slaves/" [] (handle app/get-slaves))
            (POST "/slaves/" {:keys [params]}
                (handle (partial app/add-slave params))
            )

            ; (GET "/repository/" {:keys [params]}
            ;     (handle (partial app/fetch-tag opts params))
            ; )

            (route/files "/" {:root (:resource-root opts) :allow-symlinks? true})
            (route/not-found "Not Found")
        )
    )
)

(defn start-server [opts]
    (http/run-server (app opts) {:port (:port opts)})
)
