(ns query-server.main
    (:require
        [utilities.core :as util]
        [argparser.core :as arg]
        [query-server.web-server :as web]
        [query-server.agent-scheduler :as as]
        [query-server.config :as config]
        [query-server.core :as hive]
        [query-server.mysql-connector :as mysql]
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
    (let [
        port (first (:port opts))
        dir (:dir opts)
        dir (if (empty? dir) "publics" (first dir))
        ]
        (assoc opts :port (read-string port) :dir dir)
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
                (arg/opt :conf "-conf <config>" "conf")
            ]
        }
        opts (->> args
            (arg/parse arg-spec)
            (arg/transform->map)
            (process arg-spec)
        )
        ]
        (config/set-config-path (first (:conf opts)))
        (mysql/set-mysql-db 
          (config/get-key :mysql-host)
          (config/get-key :mysql-port)
          (config/get-key :mysql-db)
          (config/get-key :mysql-user)
          (config/get-key :mysql-password)
        )
        (hive/set-hive-db
          (config/get-key :shark-host)
          (config/get-key :shark-port)
        )
        
;        (future 
;            (as/new-agent-check)
;        )
;        (future 
;            (as/inc-data-check)
;        )
;        (future
;            (do
;                (Thread/sleep 60000)
;                (as/all-data-check)
;            )
;        )
;        (future 
;            (as/mismatch-agent-check)
;        )        
        (web/start opts)
    )
)
