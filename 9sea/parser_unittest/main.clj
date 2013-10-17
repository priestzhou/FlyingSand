(ns parser-unittest.main
    (:require
        parser-unittest.parser.sql-2003
        parser-unittest.parser.translator
    )
    (:use testing.core)
    (:gen-class)
)

(defn -main [& args]
    (->> (load-cases 
            'parser-unittest.parser.sql-2003
            'parser-unittest.parser.translator
        )
        (main args)
    )
)

