(ns runtests.main
    (:require 
        [clojure.string :as str]
        [clojure.java.io :as io]
        [clansi.core :as color]
        [utilities.core :as util]
        [utilities.shutil :as sh]
        [argparser.core :as arg]
    )
    (:import 
        [java.io File]
        [java.nio.file Files FileSystems Path]
        [java.nio.file.attribute FileAttribute]
        [java.util.concurrent ArrayBlockingQueue TimeUnit]
    )
    (:gen-class)
)

(defn scanOneExeJars [work-dir exejar]
    (let [cmd ["java" "-jar" exejar "--show-cases"]
          f (.toFile (sh/getPath work-dir (format "%s.show-cases" (.getName (.toFile (sh/getPath exejar)))))) 
          {exitcode :exitcode} (sh/execute cmd :out f)
         ]
        (if (= 0 exitcode)
            (-> (slurp f) (str/split #"\n"))
            (do
                (println "execution fail: " cmd)
                (System/exit 1)
            )
        )
    )
)

(defn scanExeJars [work-dir exejars]
    (into {}
        (for [x exejars]
            (zipmap (scanOneExeJars work-dir x) (repeat x))
        )
    )
)

(def outq (ArrayBlockingQueue. 100))

(defn thread-loop [q dir]
    (when-not (empty? q)
        (let [[x & xs] q
                [cs-name exejar] x
                out (sh/getPath dir (.concat cs-name ".out"))
                {exitcode :exitcode} (sh/execute 
                    ["java" "-jar" exejar cs-name] 
                    :out out :err :out
                )
            ]
            (if (= 0 exitcode)
                (.put outq [:pass cs-name exejar])
                (.put outq [:fail cs-name exejar])
            )
            (recur xs dir)
        )
    )
)

(defn gen-queue [n cases]
    (let [qs (vec (for [_ (range n)] (transient [])))]
        (doseq [[i x] (util/enumerate cases)]
            (conj! (get qs (mod i n)) x)
        )
        (vec (for [q qs] (persistent! q)))
    )
)

(defn all-terminated? [fs]
    (reduce #(and %1 %2) (map future-done? fs))
)

(defn inner-execute [k case-num failed fs]
    (when-not (all-terminated? fs)
        (let [x (.poll outq 1 TimeUnit/SECONDS)]
            (if x
                (let [[result cs-name exejar] x]
                    (if (= result :pass)
                        (do 
                            (printf "%3d/%d [%s] %s %s" k case-num 
                                (color/style "PASS" :green) cs-name exejar
                            )
                            (println)
                        )
                        (do 
                            (conj! failed [cs-name exejar])
                            (printf "%3d/%d [%s] %s %s" k case-num 
                                (color/style "FAIL" :red) cs-name exejar
                            )
                            (println)
                        )
                    )
                    (recur (inc k) case-num failed fs)
                )
                (recur k case-num failed fs)
            )
        )
    )
)

(defn executeAll [n dir cases]
    (let [
            failed (transient [])
            qs (gen-queue n (into (sorted-map) cases))
            fs (doall (map #(future (thread-loop % dir)) qs))
        ]
        (inner-execute 1 (count cases) failed fs)
        (shutdown-agents)
        (persistent! failed)
    )
)

(defn- single-arg-as-int [opts kw default]
    (if-let [x (kw opts)]
        (merge opts {kw (Integer/parseInt (first x))})
        (merge opts {kw default})
    )
)

(defn parseArgs [args]
    (let [arg-spec {
                    :usage "Usage: [options] exejar ..."
                    :args [
                        (arg/opt :help
                             "-h|--help" "show this help message")
                        (arg/opt :cases
                             "--cases <pattern>" "a regexp which matches all cases to run, default \".*\"")
                        (arg/opt :dir
                             "-d|--dir <dir>" "a directory to put results, default \"res\"")
                        (arg/opt :parallel
                             "-j N" "parallel xkjlxflas, default 1")
                        (arg/opt :else
                             "exejar" "add exejar to run")
                        ]
                    }
          opts (arg/transform->map (arg/parse arg-spec args))]
        (when (:help opts)
            (println (arg/default-doc arg-spec))
            (System/exit 0)
        )
        (util/throw-if-not (:else opts)
            IllegalArgumentException. 
            "require at least one executable"
        )
        (single-arg-as-int opts :parallel 1)
    )
)

(defn -main [& args]
    (let [opts (parseArgs args)
            dir (if (:dir opts) (first (:dir opts)) "res")
            _ (sh/mkdir dir)
            pat (re-pattern (if (:cases opts) (first (:cases opts)) ".*"))
            exejars (:else opts)
            cases (->> exejars 
                (scanExeJars dir) 
                (filter (fn [[cs-name _]] (re-matches pat cs-name))) 
                (into {})
            )
            n (:parallel opts)
            failed (executeAll n dir cases)
        ]
        (println (format "%3d/%d failed" (count failed) (count cases)))
        (with-open [f (io/writer (format "%s/failed" dir))]
            (doseq [cs failed]
                (.write f (str/join " " cs))
                (.newLine f)
            )
        )
    )
)
