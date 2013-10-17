(ns unittest.main
    (:use testing.core)
    (:require unittest.query-server.main)
    (:gen-class)
)

(defn -main [& args]
    (->> 
        (load-cases 
;            'unittest.agent.main
            'unittest.query-server.main
        )
        (main args)
    )
)
