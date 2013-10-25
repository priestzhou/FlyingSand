(ns query-server.main
    (:require
        [utilities.core :as util]
        [argparser.core :as arg]
        [query-server.web-server :as web]
    )
    (:gen-class)
)

(defn process [arg-spec opts]
    (when (:help opts)
        (println (arg/default-doc arg-spec))
        (System/exit 0)            
    )
    (when-not (:port opts)
        (println "require -p to specify port")
        (System/exit 1)
    )
    (when-not (:dir opts)
        (println "require -d to specify directory")
        (System/exit 1)
    )
    (let [port (:port opts)]
        (assoc opts :port (read-string (first port)))
    )
)

(defn -main [& args]
    (let [
        arg-spec {
            :usage "Usage: zookeeper [topic] [group] exejar"
            :args [
                (arg/opt :help "-h|--help" "show this help")
                (arg/opt :dir 
                    "-d <dir>" "the directory where index.html and its friends are placed")
                (arg/opt :port "-p <port>" "port")
            ]
        }
        opts (->> args
            (arg/parse arg-spec)
            (arg/transform->map)
            (process arg-spec)
        )
        ]
        (web/start opts)
    )
)
