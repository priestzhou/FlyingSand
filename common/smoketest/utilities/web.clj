(ns smoketest.utilities.web
    (:use
        [testing.core :only (suite)]
        utilities.web
    )
)

(suite "try-each-until-hit"
    (:testbench
        (fn [test]
            (let [server (start-jetty {:port 11111 :join? false}
                    (handle-get "/smile" "text/plain" "hehe")
                    (handle-get "/laugh" "text/plain" "haha")
                )]
                (try
                    (test)
                (finally
                    (.stop server)
                ))
            )
        )
    )
    (:fact teuh-hit-first
        (fn []
            (http-get "http://localhost:11111/smile")
        )
        :eq
        (fn []
            {:status 200 :body "hehe"}
        )
    )
    (:fact teuh-hit-second
        (fn []
            (http-get "http://localhost:11111/laugh")
        )
        :eq
        (fn []
            {:status 200 :body "haha"}
        )
    )
    (:fact teuh-miss
        (fn []
            (http-get "http://localhost:11111/cry")
        )
        :eq
        (fn []
            {:status 404}
        )
    )
)

(suite "transitable"
    (:testbench
        (fn [test]
            (let [server (start-jetty {:port 11111 :join? false}
                    (handle-transitions
                        (handle-get "/smile" "text/plain" "hehe")
                        (handle-get "/smile" "text/plain" "xixi")
                    )
                )]
                (try
                    (test)
                (finally
                    (.stop server)
                ))
            )
        )
    )
    (:fact transitable
        (fn []
            (let [fst (http-get "http://localhost:11111/smile")
                snd (http-get "http://localhost:11111/smile")
                ]
                [fst snd]
            )
        )
        :eq
        (fn []
            [{:status 200 :body "hehe"} {:status 200 :body "xixi"}]
        )
    )
)

(suite "static-files"
    (:testbench
        (fn [test]
            (let [server (start-jetty {:port 11111 :join? false}
                    (handle-static-files {"/smile" "@/resources/open-file.txt"})
                )]
                (try
                    (test)
                (finally
                    (.stop server)
                ))
            )
        )
    )
    (:fact static-files
        (fn []
            (http-get "http://localhost:11111/smile")
        )
        :eq
        (fn []
            {:status 200 :body "haha"}
        )
    )
)
