(ns agent.util
    (:import
        [java.io StringWriter PrintWriter]
    )
)

(defn except->str [ex]
    (let [
        sw (StringWriter.)
        ]
        (with-open [wr (PrintWriter. sw)]
            (.printStackTrace ex wr)
        )
        (str sw)
    )
)
