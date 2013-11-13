(ns smoketest.main
    (:require
        smoketest.master.main
        smoketest.master.git
    )
    (:use testing.core)
    (:gen-class)
)

(defn -main [& args]
    (->> (load-cases
            'smoketest.master.main
            'smoketest.master.git
        )
        (main args)
    )
)
