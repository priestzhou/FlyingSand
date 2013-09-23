(ns monitor.tools
    (:import
        [java.io BufferedInputStream BufferedReader InputStreamReader]
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

(defn restart-process [bash]
    "df"
)

