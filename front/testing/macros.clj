(ns testing.macros
    (:require
        [clojure.string :as str]
    )
)

(defmacro throw-if [pred except & args]
    `(when ~pred
        (throw (~except ~@args))
    )
)

(defmacro throw-if-not [pred except & args]
    `(throw-if (not ~pred) ~except ~@args)
)

(defn- normalize-facts [facts]
    (if (or (empty? facts) (not= (ffirst facts) :testbench))
        ['testing.core/testbench-for-basic facts]
        (let [[[_ tb] & facts] facts]
            [tb facts]
        )
    )
)

(defn- one-fact [tb fact]
    (let [[_ cs expr rel expect] fact
            casename (str cs)
        ]
        (case rel
            :is `(do
                (def ~cs
                    (partial ~tb
                        (partial testing.core/test-is '~expr ~expr ~expect)
                    )
                )
                (swap! testing.core/testcases merge {~casename ~cs})
            )
            :eq `(do
                (def ~cs
                    (partial ~tb
                        (partial testing.core/test-eq ~expr ~expect)
                    )
                )
                (swap! testing.core/testcases merge {~casename ~cs})
            )
            :throws `(do
                (def ~cs
                    (partial ~tb
                        (partial testing.core/test-throw ~expr ~expect)
                    )
                )
                (swap! testing.core/testcases merge {~casename ~cs})
            )
        )
    )
)

(defmacro suite [& facts]
    (let [[tb facts] (normalize-facts facts)]
        (list* 'do
            (map #(one-fact tb %) facts)
        )
    )
)
