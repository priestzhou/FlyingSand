(ns utilities.web
    (:require
        [utilities.shutil :as sh]
    )
    (:use
        [ring.adapter.jetty :only (run-jetty)]
    )
    (:import
        [java.net URL HttpURLConnection]
        java.util.ArrayDeque
    )
)

; Functions in this file are for testing purpose.

(defn mime-type [f]
    (cond
        (.endsWith f ".json") "application/json"
        (.endsWith f ".js") "application/javascript"
        (.endsWith f ".gzip") "application/gzip"
        (.endsWith f ".mp3") "audio/mpeg"
        (.endsWith f ".vorbis") "audio/vorbis"
        (.endsWith f ".gif") "image/gif"
        (.endsWith f ".jpeg") "image/jpeg"
        (.endsWith f ".jpg") "image/jpeg"
        (.endsWith f ".png") "image/png"
        (.endsWith f ".svg") "image/svg+xml"
        (.endsWith f ".css") "text/css"
        (.endsWith f ".csv") "text/csv"
        (.endsWith f ".html") "text/html"
        (.endsWith f ".txt") "text/plain"
        (.endsWith f ".xml") "text/xml"
        :else (throw 
            (IllegalArgumentException. (str "unknown file type: " f))
        )
    )
)


(defn- resolve-http-response [conn]
    (let [res (.getResponseCode conn)]
        (if (>= res 400)
            {:status res}
            (merge {:status res :body (slurp (.getInputStream conn))})
        )
    )
)

(defn http-get [url]
    (let [url (URL. url)
        conn (doto (.openConnection url) (.connect))
        res (resolve-http-response conn)
        ]
        (.disconnect conn)
        res
    )
)


(defn- get-handler [path content-type result req]
    (when (and
            (= (:uri req) path)
            (= (:request-method req) :get)
        )
        {:status 200 
            :headers {
                "Access-Control-Allow-Origin" "*"
                "content-type" content-type
            }
            :body result
        }
    )
)

(defn handle-get [path content-type result]
    (partial get-handler path content-type result)
)

(defn- static-files-handler [files req]
    (when (= (:request-method req) :get)
        (let [p (:uri req)
                [f type] (files p)
            ]
            (when f
                {:status 200
                    :headers {"content-type" type}
                    :body (sh/open-file f)
                }
            )
        )
    )
)

(defn handle-static-files [files]
    (let [wrapped (apply merge 
                (for [[p f] files]
                    {p [f (mime-type f)]}
                )
            )
        ]
        (partial static-files-handler wrapped)
    )
)


(defn- try-each-until-hit' [handlers req]
    (when-not (empty? handlers)
        (let [[h & hs] handlers
                res (h req)
            ]
            (if res
                res
                (recur hs req)
            )
        )
    )
)

(defn try-each-until-hit [handlers req]
    (let [
            handlers handlers
            res (try-each-until-hit' handlers req)
        ]
        (if res
            res
            {:status 404}
        )
    )
)

(defn- transitable-handler [handlers req]
    (when-not (.isEmpty handlers)
        (let [h (.getFirst handlers)
                res (h req)
            ]
            (when res
                (.removeFirst handlers)
                res
            )
        )
    )
)

(defn handle-transitions [& handlers]
    (let [q (ArrayDeque.)]
        (doseq [x handlers]
            (.add q x)
        )
        (partial transitable-handler q)
    )
)


(defn start-jetty [opts & handlers]
    (let [server (run-jetty 
                (partial try-each-until-hit handlers) 
                opts
            )
        ]
        (Thread/sleep 1000)
        server
    )
)
