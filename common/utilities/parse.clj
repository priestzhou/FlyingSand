(ns utilities.parse
    (:use
        clojure.set
        utilities.core
    )
    (:import 
        utilities.parse.InvalidSyntaxException
    )
)

(defn- positional-stream' [s offset line row]
    (lazy-seq
        (if (empty? s)
            [[:eof offset line row]]
            (let [[x & xs] s]
                (if (= x \newline)
                    (cons [x offset line row] 
                        (positional-stream' xs (inc offset) (inc line) 1)
                    )
                    (cons [x offset line row] 
                        (positional-stream' xs (inc offset) line (inc row))
                    )
                )
            )
        )
    )
)

(defn positional-stream [str]
    (positional-stream' str 0 1 1)
)

(defn gen-ISE [stream msg]
    {
        :pre [
            (not (empty? stream))
        ]
    }
    (let [[[x _ l r] & _] stream]
        (if (= x :eof)
            (InvalidSyntaxException. msg)
            (InvalidSyntaxException. msg l r)
        )
    )
)


(defn- char-parser [pred format-eof format-char stream]
    {
        :pre [
            (not (empty? stream))
        ]
    }
    (let [[[x p l r] & xs] stream]
        (cond
            (pred x) [xs [p (inc p)]]
            (not= x :eof) (throw (InvalidSyntaxException. (format-char x) l r))
            :else (throw (InvalidSyntaxException. (format-eof)))
        )
    )
)

(defn expect-char-if [pred]
    (partial char-parser pred
        #(format "unexpected eof")
        #(format "unexpected '%c'" %)
    )
)

(defn expect-char [ch]
    (partial char-parser #(= ch %)
        #(format "expect '%c'" ch)
        #(format "expect '%c' but '%c'" ch %)
    )
)

(defn expect-no-eof []
    (expect-char-if (fn [ch] (not= ch :eof)))
)

(defn- expect-eof-parser [stream]
    {
        :pre [
            (not (empty? stream))
        ]
    }
    (let [[[x p] & xs] stream]
        (if (= x :eof)
            [xs [p p]]
            (throw (gen-ISE stream "expect eof but not"))
        )
    )
)

(defn expect-eof []
    (partial expect-eof-parser)
)

(defn- string-parser' [str stream]
    (if (empty? str)
        stream
        (let [[x & xs] str
                [strm] ((expect-char x) stream)
            ]
            (recur xs strm)
        )
    )
)

(defn- string-parser [str stream]
    (throw-if (empty? stream)
        InvalidSyntaxException. 
        (format "expect \"%s\"" str) 
    )
    (let [[[_ p] & _] stream]
        (try
            [(string-parser' str stream) [p (+ p (.length str))]]
        (catch InvalidSyntaxException ex
            (throw (gen-ISE stream (format "expect \"%s\"" str)))
        ))
    )
)

(defn expect-string [str]
    (partial string-parser str)
)

(defn- expect-string-while-parser' [pred start stream]
    (let [[[ch p] & nxt-strm] stream]
        (if (pred ch)
            (recur pred start nxt-strm)
            [stream [start p]]
        )
    )
)

(defn- expect-string-while-parser [pred stream]
    (let [[[_ start]] stream]
        (expect-string-while-parser' pred start stream)
    )
)

(defn expect-string-while [pred]
    (partial expect-string-while-parser pred)
)

(defn- choice-parser' [p stream]
    (try
        (let [[strm res] (p stream)]
            [false strm res]
        )
    (catch InvalidSyntaxException _
        [true nil nil]
    ))
)

(defn- choice-parser [parsers stream]
    (if (empty? parsers)
        (throw (gen-ISE stream "no parser can be applied"))
        (let [[p & ps] parsers
                [continue strm res] (choice-parser' p stream)
            ]
            (if continue
                (recur ps stream)
                [strm res]
            )
        )
    )
)

(defn choice [& parsers]
    (partial choice-parser parsers)
)

(defn- optional-parser [parser stream]
    (try
        (parser stream)
    (catch InvalidSyntaxException _
        [stream nil]
    ))
)

(defn optional [parser]
    (partial optional-parser parser)
)

(defn- many' [parser stream parsed]
    (try
        (let [[strm prsd] (parser stream)]
            (conj! parsed prsd)
            [true strm]
        )
    (catch InvalidSyntaxException _
        [false nil]
    ))
)

(defn- many-parser [parser stream]
    (let [parsed (transient [])]
        (loop [stream stream]
            (let [[continue strm] (many' parser stream parsed)]
                (if continue
                    (recur strm)
                    [stream (persistent! parsed)]
                )
            )
        )
    )
)

(defn many [parser]
    (partial many-parser parser)
)

(defn- many1-parser [parser stream]
    (let [[strm1 prsd1] (parser stream)
            [strm2 prsd2] ((many parser) strm1)
        ]
        [strm2 (cons prsd1 prsd2)]
    )
)

(defn many1 [parser]
    (partial many1-parser parser)
)

(defn- chain-parser [parsers stream]
    (if (empty? parsers)
        [stream []]
        (let [[p & ps] parsers
                [strm1 prsd1] (p stream)
                [strm2 prsd2] (chain-parser ps strm1)
            ]
            [strm2 (cons prsd1 prsd2)]
        )
    )
)

(defn chain [& parsers]
    (partial chain-parser parsers)
)

(defn- between-parser' [middle-prsd right-parser middle-parser stream]
    (let [[strm right-prsd] ((optional right-parser) stream)]
        (if right-prsd
            [strm right-prsd]
            (let [[strm2 mid-prsd] (middle-parser stream)]
                (conj! middle-prsd mid-prsd)
                (recur middle-prsd right-parser middle-parser strm2)
            )
        )
    )
)

(defn- between-parser [left-parser right-parser middle-parser stream]
    (let [[strm1 prsd1] (left-parser stream)
            middle-prsd (transient [])
            [strm2 right-prsd] 
                (between-parser' middle-prsd right-parser middle-parser strm1)
        ]
        [strm2 [prsd1 right-prsd (persistent! middle-prsd)]]
    )
)

(defn between [left-parser right-parser middle-parser]
    (partial between-parser left-parser right-parser middle-parser)
)

(defn foresee-parser [parser stream]
    (let [[_ prsd] (parser stream)]
        [stream prsd]
    )
)

(defn foresee [parser]
    (partial foresee-parser parser)
)


(def digit #{\0 \1 \2 \3 \4 \5 \6 \7 \8 \9})

(def hexdigit (union #{\a \b \c \d \e \f \A \B \C \D \E \F} digit))

(def letter 
    (set 
        (for [x (range 128)
                :let [ch (char x)]
                :let [gta (>= (Character/compare ch \a) 0)] 
                :let [ltz (<= (Character/compare ch \z) 0)]
                :let [gtA (>= (Character/compare ch \A) 0)]
                :let [ltZ (<= (Character/compare ch \Z) 0)]
                :when (or
                    (and gta ltz)
                    (and gtA ltZ)
                )
            ]
            ch
        )
    )
)

(def whitespace #{\space \tab \formfeed \newline})

(defn- extract-string-between' [sb end-stream start-stream]
    (if (= start-stream end-stream)
        (str sb)
        (let [[[ch] & rest-stream] start-stream]
            (throw-if (= ch :eof)
                IllegalArgumentException.
                "stream ends before it hits end position"
            )
            (.append sb ch)
            (recur sb end-stream rest-stream)
        )
    )
)

(defn extract-string-between [start-stream end-stream]
    (let [sb (StringBuilder.)]
        (extract-string-between' sb end-stream start-stream)
    )
)
