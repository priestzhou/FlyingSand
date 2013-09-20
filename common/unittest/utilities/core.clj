(ns unittest.utilities.core
    (:use testing.core
        utilities.core
    )
    (:import
        [java.util ArrayList]
    )
)

(suite "zip several seqs together"
    (:fact zip-1 (zip [1 2 3] ["a" "b" "c"]) :is [[1 "a"] [2 "b"] [3 "c"]])
    (:fact zip-2 (zip [1 2] ["a" "b" "c"]) :is [[1 "a"] [2 "b"]])
    (:fact zip-3 (zip [] []) :is [])
)

(suite "enumerate: like its python relatives"
    (:fact enumerate-1 (enumerate ["a" "b" "c"]) :is [[0 "a"] [1 "b"] [2 "c"]])
    (:fact enumerate-2 (enumerate 5 ["a" "b" "c"]) :is [[5 "a"] [6 "b"] [7 "c"]])
    (:fact enumerate-3 (enumerate []) :is [])
)

(suite "iterator and iterable"
    (:fact lazy-seq-from-iterator:empty
        (iterator->lazy-seq! (.iterator (ArrayList.)))
        :is
        []
    )
    (:fact lazy-seq-from-iterator:one
        (iterator->lazy-seq! (.iterator (doto (ArrayList.) (.add "a"))))
        :is
        ["a"]
    )
    (:fact lazy-seq-from-iterator:two
        (iterator->lazy-seq! (.iterator (doto (ArrayList.) (.add "a") (.add "b"))))
        :is
        ["a" "b"]
    )
    (:fact lazy-seq-from-iterable
        (iterable->lazy-seq (doto (ArrayList.) (.add "a") (.add "b")))
        :is
        ["a" "b"]
    )
)
