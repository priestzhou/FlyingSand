(ns logging.main
    (:use
        [logging.core :only [defloggers]]
    )
    (:import
        [org.slf4j LoggerFactory Logger]
    )
    (:gen-class)
)

(defloggers debug info warn error)

(defn -main [& args]
    (debug "xixi" :key "smile")
    (info "hehe" :key "smile")
    (warn "hoho" :key "smile")
    (try
        (/ 1 0)
    (catch Throwable ex
        (error "haha" :exception ex)
    ))
    (info "aaa" #{:key "smile"})
    (info "bbb" {:key "smile"})
    (info "ccc" [:key "smile"])
)
