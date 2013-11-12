(ns master.main
    (:require
        [argparser.core :as arg]
        [master.app :as app]
        [master.web :as web]
    )
    (:use
        [compojure.core :only (defroutes GET PUT POST DELETE HEAD ANY)]
        [slingshot.slingshot :only (try+ throw+)]
        [logging.core :only [defloggers]]
    )
    (:gen-class)
)

(defloggers debug info warn error)

(defn parseArgs [args]
    (let [arg-spec {
            :usage "Usage:"
            :args []
        }
        opts (arg/transform->map (arg/parse arg-spec args))
        ]
        opts
    )
)

(defn -main [& args]
    (warn "start")
    (parseArgs args)
    (let [opts {
            :resource-root "publics"
            :slaves "slaves"
            :port 9999
        }
        ]
        (app/init-slaves opts)
        (web/start-server opts)
    )
)
