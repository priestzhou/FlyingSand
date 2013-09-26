(ns monitor.tools
    (:import
        [java.io BufferedInputStream BufferedReader InputStreamReader]
    )
    (:require
        [utilities.shutil :as sh]
    )
)

(defn check-process [tag]
    (let [
            cmd  (into-array 
                    ["/bin/sh" "-c" 
                        (str "ps -ef |grep -v grep |grep " tag)
                    ]
                )
            run (Runtime/getRuntime)
            p (.exec run cmd)
            out (->>
                    (.getInputStream p)
                    BufferedInputStream.
                    InputStreamReader.
                    BufferedReader.
                    .readLine
                )
        ]
        out
    )
)

 (defn now [] (new java.util.Date))

(defn restart-process [bash]
    (let [
            cmd  (into-array 
                    ["/bin/sh" "-c" 
                        bash ";"
                    ]
                )
            run (Runtime/getRuntime)
            ;p (apply (partial sh/execute " java ") bash)
            t1 (println "t1" (now) )
            p (.exec run cmd)
            t2 (println "t2" (now) )
        ]
        p
    )
)

(defn check [tag bash sleeptime]
    (Thread/sleep sleeptime)
    (let [flag (check-process tag)
        ]
        (if (nil? flag)
            (restart-process bash)
        )
    )
    (recur tag bash sleeptime)
)
