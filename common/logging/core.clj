(ns logging.core
    (:require 
        [utilities.core :as util]
    )
    (:import 
        [org.slf4j LoggerFactory Logger]
    )
)

(defn- repack [args]
    (cond
        (set? args) (set (map repack args))
        (map? args) (apply hash-map (reduce concat (map repack args)))
        (seq? args) (vec (map repack args))
        (instance? Throwable args) (do
            (util/format-stack-trace args)
        )
        :else (prn-str args)
    )
)

(defn- logfn [enabled? log msg args]
    (when (enabled?)
        (let [s (print-str (repack (cons msg args)))]
            (log s)
        )
    )
)

(defn new-loggers [path]
    (let [logger (LoggerFactory/getLogger path)]
        {
            :debug (fn [msg & args] 
                (logfn #(.isDebugEnabled logger) #(.debug logger %) msg args)
            )
            :info (fn [msg & args] 
                (logfn #(.isInfoEnabled logger) #(.info logger %) msg args)
            )
            :warn (fn [msg & args] 
                (logfn #(.isWarnEnabled logger) #(.warn logger %) msg args)
            )
            :error (fn [msg & args] 
                (logfn #(.isErrorEnabled logger) #(.error logger %) msg args)
            )
        }
    )
)

(defmacro defloggers [debug info warn error] 
    `(do
        (def ^:private loggers# (#'logging.core/new-loggers ~(str *ns*)))
        (def ^:private ~debug (:debug loggers#))
        (def ^:private ~info (:info loggers#))
        (def ^:private ~warn (:warn loggers#))
        (def ^:private ~error (:error loggers#))
    )
)
