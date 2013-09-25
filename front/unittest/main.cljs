(ns unittest.main
    (:require
        [testing.core :as test]
        [utilities.core :as util]
    )
    (:use-macros
        [testing.macros :only (suite)]
    )
)

(suite
    (:fact zip:equal-length 
        (util/zip [1 2 3] ["a" "b" "c"]) :is [[1 "a"] [2 "b"] [3 "c"]]
    )
    (:fact zip:unequal-length 
        (util/zip [1 2] ["a" "b" "c"]) :is [[1 "a"] [2 "b"]]
    )
    (:fact zip:empty 
        (util/zip [] []) :is []
    )
)

(suite
    (:fact enumerate:default 
        (util/enumerate ["a" "b" "c"]) :is [[0 "a"] [1 "b"] [2 "c"]]
    )
    (:fact enumerate:start-index 
        (util/enumerate 5 ["a" "b" "c"]) :is [[5 "a"] [6 "b"] [7 "c"]]
    )
    (:fact enumerate:empty 
        (util/enumerate []) :is []
    )
)

(suite
    (:fact jsarray
        (->> ["a" 1]
            (util/->js-obj)
            (util/->cljs-coll)
        )
        :is
        ["a" 1]
    )
    (:fact jsmap
        (->> {"a" 1}
            (util/->js-obj)
            (util/->cljs-coll)
        )
        :is
        {"a" 1}
    )
)

(suite
    (:fact nested-merge
        (util/nested-merge {:t {:a 1 :b 2}} {:t {:a 0 :c 3}})
        :is
        {:t {:a 0 :b 2 :c 3}}
    )
)

(test/main)
