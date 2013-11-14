(ns query-server.config
(:use  
  [logging.core :only [defloggers]]
)
)

(defloggers debug info warn error)

; need a placeholder to make the compile success 
(def ^:private config-path (ref "./webserver_props.clj") )

(defn set-config-path
  [config-file-path]
  (if-not (nil? config-file-path)
    (
     dosync (ref-set config-path config-file-path)
     (prn "config-file-path" @config-path)
    )
  )
)

(defn get-config
	[conf-file]
  (try
    (read-string 
      (slurp conf-file
      )
    )
  (catch Exception e
    (error "read configuration failed!" :error-msg (.getMessage e))
  )
  )
)

(defn get-key
  "Get single key value pair from config file."
 [key]
 (get (get-config @config-path) key)
) 

;(defn get-

