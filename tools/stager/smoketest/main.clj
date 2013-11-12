(ns smoketest.main
    (:require
        smoketest.master.main
    )
    (:use testing.core)
    (:gen-class)
)

(defn -main [& args]
    (->> (load-cases
            'smoketest.master.main
        )
        (main args)
    )
)
