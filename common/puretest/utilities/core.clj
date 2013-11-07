(ns puretest.utilities.core
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
)

(suite "array->lazy-seq"
    (:fact array->lazy-seq:String
        (->> ["a" "b" "c"]
            (into-array String)
            (array->lazy-seq)
            (doall)
        )
        :is
        ["a" "b" "c"]
    )
    (:fact array->lazy-seq:byte
        (->> [(byte 0) (byte 1) (byte 2)]
            (byte-array)
            (array->lazy-seq)
            (doall)
        )
        :is
        [0 1 2]
    )
)

(suite "low-byte->char"
    (:fact low-byte->char:0
        (low-byte->char 0) :is \0
    )
    (:fact low-byte->char:9
        (low-byte->char 9) :is \9
    )
    (:fact low-byte->char:a
        (low-byte->char 10) :is \a
    )
    (:fact low-byte->char:f
        (low-byte->char 15) :is \f
    )
)

(suite "byte->digits"
    (:fact byte->digits!:zero
        (let [sb (StringBuilder.)]
            (byte->digits! sb 0)
            (str sb)
        )
        :is "00"
    )
    (:fact byte->digits!:positive
        (let [sb (StringBuilder.)]
            (byte->digits! sb 1)
            (str sb)
        )
        :is "01"
    )
    (:fact byte->digits!:negative
        (let [sb (StringBuilder.)]
            (byte->digits! sb -1)
            (str sb)
        )
        :is "ff"
    )
)

(suite "hexdigits"
    (:fact hexdigits:empty
        (hexdigits (byte-array [])) :is ""
    )
    (:fact hexdigits:one
        (hexdigits (byte-array [(byte 0x12)])) :is "12"
    )
    (:fact hexdigits:two
        (hexdigits (byte-array [(byte 0x12) (byte -2)])) :is "12fe"
    )
)