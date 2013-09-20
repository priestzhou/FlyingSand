(ns smoketest.main
    (:use testing.core)
    (:require 
        smoketest.utilities.shutil
        smoketest.zktools.core
        smoketest.kfktools.core
        smoketest.utilities.web
        smoketest.utilities.net
    )
    (:gen-class)
)

(defn -main [& args]
    (->> (load-cases 
            'smoketest.utilities.shutil
            'smoketest.utilities.web
            'smoketest.utilities.net
            'smoketest.zktools.core
            'smoketest.kfktools.core
        )
        (main args)
    )
)
