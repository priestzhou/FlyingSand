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

(defn low-byte->char [b]
{
    :pre [(<= 0 b 15)]
}
    (cond
        (<= 0 b 9) (char (+ b 48))
        :else (char (+ b 87))
    )
)

(defn byte->digits! [^StringBuilder sb b]
{
    :pre [(<= -128 b 127)]
}
    (if-not (neg? b)
        (do
            (.append sb (low-byte->char (quot b 16)))
            (.append sb (low-byte->char (rem b 16)))
        )
        (let [b (+ b 256)]
            (.append sb (low-byte->char (quot b 16)))
            (.append sb (low-byte->char (rem b 16)))
        )
    )
)

(defn hexdigits [^bytes bytes]
    (let [sb (StringBuilder. (* 2 (alength bytes)))]
        (doseq [b (array->lazy-seq bytes)]
            (byte->digits! sb b)
        )
        (str sb)
    )
)


