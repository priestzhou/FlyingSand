(ns argparser.core
  (:require 
      [clojure.string :as str]
      [utilities.core :as util]
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
        :pre [(pos? n)
            (not (empty? args))
        ]
    }
    (if-not (opts (first args))
        [nil args]
        (do
            (util/throw-if-not (>= (count args) n)
                IllegalArgumentException.
                (str "require " (dec n) " args after " (first args))
            )
            [
                {key (apply vector (rest (take n args)))} 
                (apply vector (drop n args))
            ]
        )
    )
)

(defn- gen-parser [key option]
    (let [args (str/split option #" ")
            opt? (= (.charAt (first args) 0) \-)
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
                      :when (not= nil result)
                  ] 
                  [result remains]
                ))
            ]
            ret 
            [nil options]
        )
    )
)

(defn parse
    "parse options with the given spec"
    [{:keys [usage args]} options]
    (loop [ret [] 
           [result remains] (parse-once args options)]
        (if (nil? result)
            (do
                (util/throw-if-not (empty? remains)
                 IllegalArgumentException.
                 (str "Unknown option: " (first remains))
                )
                ret
            )
            (recur (conj ret result) (parse-once args remains))
        )
    )
)

(defn transform->map
    "default transform from the raw parser result to a map from key to vals"
    [args]
    (apply merge-with concat args)
)

(defn select-args
    "select opts from the spec"
    [selected-keys all]
    (filter #((set selected-keys) (:key %)) all)
)

(defn- gen-white-spaces 
    [size]
    (str/join (for [i (range size)] " "))
)

(defn- gen-str-vector
    [x]
    (cond 
        (nil? x) []
        (vector? x) x
        (string? x) [x]
        :else (vec x)
    )
)

(defn- args->max-width
    "return max option length"
    [args]
    (->> (map :desc args)
        (map (comp count first))
        (apply max)
    )
)

(defn doc-args
    "generate help msg for the given args"
    [args max-width]
    (if (nil? args) 
        []
        (for [
                desc (map :desc args) 
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
    [{:keys [usage synopsys args summary]}] ;input is spec of the parser
    (let [emptyline [""]
            max-width (args->max-width args)
            doc->usage   (gen-str-vector usage)
            doc->synopsys (gen-str-vector synopsys)
            doc->args     (doc-args args max-width)
            doc->summary  (gen-str-vector summary)
            doc (flatten [doc->usage

                    (when-not (empty? doc->synopsys) emptyline)
                    doc->synopsys
                   
                    (when-not (empty? doc->args) emptyline)
                    (when-not (empty? doc->args) ["Options:"])
                    doc->args
                   
                    (when-not (empty? doc->summary) emptyline)
                    doc->summary
                ]
            )
        ]
        (str/join "\n" doc)
    )
)

