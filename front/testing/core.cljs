(ns testing.core
    (:use-macros
        [testing.macros :only (throw-if throw-if-not)]
    )
)

(def testcases (atom {}))

(defn testbench-for-basic [test]
    (test)
)

(defn test-is [expr x y & _]
    (if (not= x y)
        (throw (js/Error. 
            (format "%s expect %s but %s" (pr-str expr) (pr-str x) (pr-str y))
        ))
    )
)

(defn test-eq [f g & args]
    (let [x (apply f args)
            y (apply g args)
        ]
        (if (not= x y)
            (throw (js/Error.
                (format "apply %s to %s expect %s but %s"
                    (pr-str args)
                    (pr-str f)
                    (pr-str y)
                    (pr-str x)
                )
            ))
        )
    )
)

(defn- test-throw' [f args]
    (try
        (apply f args)
        nil
    (catch js/Error ex
        ex
    ))
)

(defn- test-throw-except [f ex-type args]
    (let [e (test-throw' f args)]
        (cond
            (nil? e) (throw (js/Error.
                (format "apply %s to %s expect %s, but nothing happened."
                    (pr-str args)
                    (pr-str f)
                    (pr-str ex-type)
                )
            ))
            (not (instance? ex-type e)) (throw (js/Error.
                (format "apply %s to %s expect %s, but %s happened."
                    (pr-str args)
                    (pr-str f)
                    (pr-str ex-type)
                    (pr-str e)
                )
            ))
        )
    )
)

(defn- test-throw-nothing [f args]
    (let [e (test-throw' f args)]
        (when-not (nil? e)
            (throw (js/Error.
                (format "apply %s to %s expect no exception but %s happened"
                    (pr-str args)
                    (pr-str f)
                    (pr-str e)
                )
            ))
        )
    )
)

(defn test-throw [f ex-type & args]
    (if (= ex-type :nothing)
        (test-throw-nothing f args)
        (test-throw-except f ex-type args)
    )
)


(defn parseArgs []
    (let [sys (js/require "system")
            args (.-args sys)
        ]
        (when-not (= (count args) 2)
            (.log js/console "Usage: [phantomjs|slimerjs] this.js [--show-cases|case]")
            (.exit js/phantom 1)
        )
        (second args)
    )
)

(defn main []
    (let [arg (parseArgs)]
        (cond
            (#{"-h" "--help"} arg) (do
                (.log js/console "Usage: [phantomjs|slimerjs] this.js [--show-cases|case]")
                (.exit js/phantom 0)
            )
            (= arg "--show-cases") (do
                (doseq [[k] @testcases]
                    (.log js/console k)
                )
                (.exit js/phantom 0)
            )
            :else (let [cs (@testcases arg)]
                (try
                    (cs)
                    (.exit js/phantom 0)
                (catch js/Error ex
                    (.log js/console ex)
                    (.exit js/phantom 1)
                ))
            )
        )
    )
)
