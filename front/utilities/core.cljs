(ns utilities.core
)

(defn zip [& args]
    (apply map vector args)
)

(defn enumerate 
    ([init xs]
    (zip (iterate inc init) xs))
    
    ([xs]
    (enumerate 0 xs))
)


(declare ->js-obj)

(defn- ->js-array [arr]
    (apply array (map ->js-obj arr))
)

(defn- ->js-map [dict]
    (apply js-obj (flatten 
        (for [[k v] dict] [(name k) (->js-obj v)])
    ))
)

(defn ->js-obj [o]
    (cond
        (vector? o) (->js-array o)
        (map? o) (->js-map o)
        :else o
    )
)


(declare ->cljs-coll)

(defn ->cljs-vec [arr]
    (let [res (transient [])]
        (doseq [i (range (count arr))]
            (conj! res (->cljs-coll (aget arr i)))
        )
        (persistent! res)
    )
)

(defn ->cljs-map [m]
    (let [res (transient {})
        ks (.keys js/Object m)
        ]
        (doseq [i (range (count ks))
            :let [k (aget ks i)]
            ]
            (assoc! res k (->cljs-coll (aget m k)))
        )
        (persistent! res)
    )
)

(defn ->cljs-coll [o]
    (cond
        (instance? js/Array o) (->cljs-vec o)
        ; this implementation is buggy because js array can be indexed by anything, 
        ; rather than only non-negative integers
        (instance? js/Object o) (->cljs-map o)
        :else o
    )
)


(defn nested-merge [& maps]
    (apply merge-with (fn [val-in-result val-in-latter]
            (cond
                (and (map? val-in-result) (map? val-in-latter))
                    (nested-merge val-in-result val-in-latter)
                :else val-in-latter
            )
        )
        maps
    )
)
