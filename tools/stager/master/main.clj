(ns master.main
    (:require
        [argparser.core :as arg]
        [utilities.shutil :as sh]
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

(defn -main [& args]
    (warn "start")
    (let [opts {
            :resource-root (sh/getPath "publics")
            :slaves (sh/getPath "slaves")
            :port 9999
        }
        ]
        (app/init opts)
        (web/start-server opts)
    )
)
