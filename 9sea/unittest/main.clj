(ns unittest.main
    (:use testing.core)
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
