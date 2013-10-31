(ns parser-puretest.main
    (:require
        parser-puretest.parser.sql-2003
        parser-puretest.parser.translator
    )
    (:use testing.core)
    (:gen-class)
)

(defn -main [& args]
    (->> (load-cases
            'parser-puretest.parser.sql-2003
            'parser-puretest.parser.translator
        )
        (main args)
    )
)

