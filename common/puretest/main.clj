(ns puretest.main
    (:require
        puretest.utilities.core
        puretest.utilities.shutil
        puretest.utilities.parse
        puretest.utilities.aes
        puretest.argparser.core
    )
    (:use testing.core)
    (:gen-class)
)

(defn -main [& args]
    (->> (load-cases
            'puretest.utilities.core
            'puretest.utilities.shutil
            'puretest.utilities.parse
            'puretest.utilities.aes
            'puretest.argparser.core)
        (main args)
    )
)
