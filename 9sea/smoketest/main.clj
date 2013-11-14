(ns smoketest.main
    (:use testing.core
        smoketest.monitor.main
        smoketest.query-server.main
    )
    (:gen-class)
)

(defn -main [& args]
    (->> (load-cases 
            'smoketest.monitor.main
            'smoketest.query-server.main
        )
        (main args)
    )
)
