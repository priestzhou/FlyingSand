(ns parser.main
    (:require
        [utilities.core :as util]
        [argparser.core :as arg]
    )
    (:use
        [parser.translator :only (sql-2003->hive)]
    )
    (:gen-class)
)

(defn parseArgs [args]
    (let [
        arg-spec {
            :usage "Usage: [options] exejar ..."
            :args [
                (arg/opt :help
                     "-h|--help" "show this help message")
                (arg/opt :context
                     "--context file" "context file")
                (arg/opt :query
                     "query" "SQL query")
            ]
        }
        opts (arg/transform->map (arg/parse arg-spec args))
        ]
        (when (:help opts)
            (println (arg/default-doc arg-spec))
            (System/exit 0)
        )
        (util/throw-if-not (:query opts)
            IllegalArgumentException. 
            "require query"
        )
        (util/throw-if-not (:context opts)
            IllegalArgumentException. 
            "require context"
        )
        opts
    )
)

(defn -main [& args]
    (let [
        opts (parseArgs args)
        context (->> opts
            (:context)
            (first)
            (slurp)
            (read-string)
        )
        query (->> opts
            (:query)
            (first)
        )
        ]
        (println
            (sql-2003->hive context query)
        )
    )
)
