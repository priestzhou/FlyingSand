(ns unittest.main
    (:use testing.core
        unittest.agent.main
    )
    (:gen-class)
)

(defn -main [& args]
    (->> 
        (load-cases 
            'unittest.agent.main
        )
        (main args)
    )
)
