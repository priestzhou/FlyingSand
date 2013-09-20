(ns utilities.core
    (:import 
        [java.nio.charset StandardCharsets]
        [java.io StringWriter PrintWriter]
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

(defn zip [& args]
    (apply map vector args)
)

(defn enumerate 
    ([init xs]
    (zip (iterate inc' init) xs))
    
    ([xs]
    (enumerate 0 xs))
)

(defn format-stack-trace [^Throwable ex]
    (let [s (StringWriter.)
            wrt (PrintWriter. s)
        ]
        (.printStackTrace ex wrt)
        (.close wrt)
        (str s)
    )
)

(defn iterator->lazy-seq! [^java.util.Iterator it]
    (if-not (.hasNext it)
        []
        (lazy-seq
            (let [nxt (.next it)]
                (cons nxt (iterator->lazy-seq! it))
            )
        )
    )
)

(defn iterable->lazy-seq [^Iterable it]
    (iterator->lazy-seq! (.iterator it))
)

(defn- array-lazy-seq' [arr i]
    (if (= i (alength arr))
        []
        (lazy-seq
            (cons (aget arr i) (array-lazy-seq' arr (inc i)))
        )
    )
)

(defn array->lazy-seq [arr]
    (array-lazy-seq' arr 0)
)


(defn bytes->str ^String [^bytes b]
    (String. b StandardCharsets/UTF_8)
)

(defn str->bytes ^bytes [^String s]
    (.getBytes s StandardCharsets/UTF_8)
)
