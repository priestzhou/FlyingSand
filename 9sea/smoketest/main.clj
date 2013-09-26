(ns smoketest.main
    (:use testing.core
        smoketest.monitor.main
    )
    (:gen-class)
)

(defn -main [& args]
    (->> (load-cases 
            'smoketest.monitor.main
        )
        (main args)
    )
)
