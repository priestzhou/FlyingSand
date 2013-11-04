(ns argparser.core
    (:require
        [clojure.string :as str]
        [utilities.core :as util]
        [utilities.shutil :as sh]
    )
)

(defn- parse-arg [key args]
{
    :pre [(not (empty? args))]
}
    [{key [(first args)]} (rest args)]
)

(defn- parse-opt [opts n key args]
{
    :pre [
        (pos? n)
        (not (empty? args))
    ]
}
    (if-not (opts (first args))
        [nil args]
        (do
            (util/throw-if (< (count args) n)
                IllegalArgumentException.
                (str "require " (dec n) " args after " (first args))
            )
            [
                {key (rest (take n args))}
                (drop n args)
            ]
        )
    )
)

(defn- gen-parser [key option]
    (let [args (str/split option #" ")
            opt? (= (ffirst args) \-)
        ]
        (if-not opt?
            (partial parse-arg key)
            (let [opts (set (str/split (first args) #"[|]"))
                    n (count args)
                ]
                (partial parse-opt opts n key)
            )
        )
    )
)

(defn opt
    "handler for one option"
    [key option & description]
    {:key key :desc [option (vec description)] :parse (gen-parser key option)}
)

(defn- parse-once
    "apply each opt's parse functon on the coming option"
    [opts options]
    (if (or (empty? opts) (empty? options))
        [nil options]
        (if-let [ret (first
                (for [opt opts
                      :let [[result remains] ((:parse opt) options)]
                      :when result
                  ]
                  [result remains]
                ))
            ]
            ret
            [nil options]
        )
    )
)

(defn- in? [key args]
    (first (for [
        arg args
        :when (get arg key)
        ]
        arg
    ))
)

(defn- parse' [opts ret args]
    (if (empty? args)
        ret
        (let [[res remains] (parse-once opts args)]
            (util/throw-if (nil? res)
                IllegalArgumentException.
                (str "Unknown option: " (first args))
            )
            (recur opts (conj ret res) remains)
        )
    )
)

(declare default-doc)

(defn parse
    "parse options with the given spec"
    [{:keys [usage args] :as arg-spec} options]
    (let [
        args (concat [
                (opt :help "-h|--help" "show this help message")
                (opt :build-info "--buildinfo" "show build info")
            ]
            (for [
                arg args
                :when (not (#{:help :build-info} (:key arg)))
                ]
                arg
            )
        )
        res (parse' args [] options)
        ]
        (when (in? :help res)
            (-> arg-spec
                (assoc :args args)
                (default-doc)
                (println)
            )
            (System/exit 0)
        )
        (when (in? :build-info res)
            (->> "@/manifest"
                (sh/open-file)
                (slurp)
                (re-find #"Build-Info:\s+(\w+)")
                (second)
                (println)
            )
            (System/exit 0)
        )
        res
    )
)

(defn transform->map
    "default transform from the raw parser result to a map from key to vals"
    [args]
    (apply merge-with concat args)
)

(defn- gen-white-spaces [size]
    (str/join (for [_ (range size)] " "))
)

(defn- gen-str-vector [x]
    (cond
        (nil? x) []
        (vector? x) x
        (string? x) [x]
        :else (vec x)
    )
)

(defn- args-max-width
    "return max option length"
    [args]
    (->> args
        (map :desc)
        (map first)
        (map count)
        (apply max)
    )
)

(defn- doc-args [args]
    (let [max-width (args-max-width args)]
        (for [
            arg args
            :let [desc (:desc arg)]
            [n explain] (util/enumerate (second desc))
            :let [opt (first desc)]
            ]
            (str/join (flatten [
                " "
                (if (= n 0)
                    [opt (gen-white-spaces (- max-width (count opt)))]
                    (gen-white-spaces max-width)
                )
                "  "
                explain
            ]))
        )
    )
)

(defn default-doc
    "default help msg generator"
    [{:keys [usage synopsys args summary]}]
    (let [emptyline [""]
            doc-usage   (gen-str-vector usage)
            doc-synopsys (gen-str-vector synopsys)
            doc-args     (doc-args args)
            doc-summary  (gen-str-vector summary)
            doc (flatten [doc-usage

                    (when-not (empty? doc-synopsys) emptyline)
                    doc-synopsys

                    (when-not (empty? doc-args) [emptyline "Options:"])
                    doc-args

                    (when-not (empty? doc-summary) emptyline)
                    doc-summary
                ]
            )
        ]
        (str/join "\n" doc)
    )
)

(defn extract-with-default [opts kw proc default]
    (if-let [v (get opts kw)]
        (assoc opts kw (proc v))
        (assoc opts kw default)
    )
)
