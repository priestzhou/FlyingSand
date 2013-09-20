(ns unittest.main
    (:require 
        unittest.utilities.core
        unittest.utilities.shutil
        unittest.utilities.parse
        unittest.argparser.core
    )
    (:use testing.core)
    (:gen-class)
)

(defn -main [& args]
    (->> (load-cases 
            'unittest.utilities.core 
            'unittest.utilities.shutil 
            'unittest.utilities.parse
            'unittest.argparser.core)
        (main args)
    )
)
