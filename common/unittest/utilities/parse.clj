(ns unittest.utilities.parse
    (:use 
        testing.core
        utilities.parse
    )
    (:import
        [utilities.parse InvalidSyntaxException]
    )
)

(suite "InvalidSyntaxException"
    (:fact ISE-throw
        (fn [] (throw (InvalidSyntaxException. "msg" 1 2)))
        :throws
        InvalidSyntaxException
    )
    (:fact ISE-access-ISE
        (try
            (throw (InvalidSyntaxException. "msg" 1 2))
        (catch InvalidSyntaxException ex
            [(.getRawMessage ex) (.getLine ex) (.getRow ex) (.getMessage ex)]
        ))
        :is
        ["msg" 1 2 "msg at 1:2"]
    )
)

(suite "positional-stream"
    (:fact positional-stream
        (positional-stream "ab\nc")
        :is
        [[\a 0 1 1] [\b 1 1 2] [\newline 2 1 3] [\c 3 2 1] [:eof 4 2 2]]
    )
)

(suite "expect-char"
    (:fact char-match
        (
            (expect-char \a)
            (positional-stream "abc")
        )
        :is
        [[[\b 1 1 2] [\c 2 1 3] [:eof 3 1 4]] [0 1]]
    )
    (:fact char-eof
        (fn []
            (
                (expect-char \a)
                (positional-stream "")
            )
        )
        :throws
        InvalidSyntaxException
    )
    (:fact char-unmatch
        (fn []
            (
                (expect-char \b)
                (positional-stream "abc")
            )
        )
        :throws
        InvalidSyntaxException
    )
)

(suite "expect-char-if"
    (:fact char-if-match
        (
            (expect-char-if #{\a})
            (positional-stream "ab")
        )
        :is
        [[[\b 1 1 2] [:eof 2 1 3]] [0 1]]
    )
    (:fact char-if-unmatch
        (fn []
            (
                (expect-char-if #{\a})
                (positional-stream "ba")
            )
        )
        :throws InvalidSyntaxException
    )
)

(suite "digit"
    (:testbench 
        (fn [test]
            (doseq [x (range 128)]
                (test (char x))
            )
        )
    )
    (:fact digit
        (fn [ch]
            (boolean (digit ch))
        )
        :eq
        (fn [ch]
            (Character/isDigit ch)
        )
    )
)

(suite "letter"
    (:testbench 
        (fn [test]
            (doseq [x (range 128)]
                (test (char x))
            )
        )
    )
    (:fact letter
        (fn [ch]
            (boolean (letter ch))
        )
        :eq
        (fn [ch]
            (Character/isLetter ch)
        )
    )
)

(suite "expect-no-eof"
    (:fact no-eof-match
        (
            (expect-no-eof)
            (positional-stream "a")
        )
        :is
        [[[:eof 1 1 2]] [0 1]]
    )
    (:fact no-eof-eof
        (fn []
            (
                (expect-no-eof)
                (positional-stream "")
            )
        )
        :throws InvalidSyntaxException
    )
)

(suite "expect-eof"
    (:fact eof-eof
        (
            (expect-eof)
            (positional-stream "")
        )
        :is
        [nil [0 0]]
    )
    (:fact eof-noeof
        (fn []
            (
                (expect-eof)
                (positional-stream "abc")
            )
        )
        :throws
        InvalidSyntaxException
    )
)

(suite "expect-string"
    (:fact str-normal
        (
            (expect-string "ab") 
            (positional-stream "abc")
        )
        :is
        [[[\c 2 1 3] [:eof 3 1 4]] [0 2]]
    )
    (:fact str-eof
        (fn []
            (
                (expect-string "ab") 
                (positional-stream "")
            )
        )
        :throws
        InvalidSyntaxException
    )
    (:fact str-short
        (fn []
            (
                (expect-string "ab") 
                (positional-stream "a")
            )
        )
        :throws
        InvalidSyntaxException
    )
    (:fact str-unmatch
        (fn []
            (
                (expect-string "ab") 
                (positional-stream "acb")
            )
        )
        :throws
        InvalidSyntaxException
    )
)

(suite "expect-string-while"
    (:fact expect-string-while-normal
        (
            (expect-string-while digit)
            (positional-stream "1a")
        )
        :is
        [[[\a 1 1 2] [:eof 2 1 3]] [0 1]]
    )
    (:fact expect-string-while-empty
        (
            (expect-string-while digit)
            (positional-stream "a")
        )
        :is
        [[[\a 0 1 1] [:eof 1 1 2]] [0 0]]
    )
)

(suite "choice"
    (:fact choice-first
        (
            (choice (expect-char \a) (expect-string "ab"))
            (positional-stream "abc")
        )
        :is
        [[[\b 1 1 2] [\c 2 1 3] [:eof 3 1 4]] [0 1]]
    )
    (:fact choice-second
        (
            (choice (expect-char \a) (expect-string "bc"))
            (positional-stream "bcd")
        )
        :is
        [[[\d 2 1 3] [:eof 3 1 4]] [0 2]]
    )
    (:fact choice-unmatch
        (fn []
            (
                (choice (expect-char \a) (expect-string "bc"))
                (positional-stream "xyz")
            )
        )
        :throws
        InvalidSyntaxException
    )
)

(suite "optional"
    (:fact optional-match
        (
            (optional (expect-char \a))
            (positional-stream "ab")
        )
        :is
        [[[\b 1 1 2] [:eof 2 1 3]] [0 1]]
    )
    (:fact optional-unmatch
        (
            (optional (expect-char \z))
            (positional-stream "ab")
        )
        :is
        [[[\a 0 1 1] [\b 1 1 2] [:eof 2 1 3]] nil]
    )
)

(suite "many"
    (:fact many-one
        (
            (many (expect-char \a)) 
            (positional-stream "ab")
        )
        :is
        [[[\b 1 1 2] [:eof 2 1 3]] [[0 1]]]
    )
    (:fact many-many
        (
            (many (expect-char \a))
            (positional-stream "aab")
        )
        :is
        [[[\b 2 1 3] [:eof 3 1 4]] [[0 1] [1 2]]]
    )
    (:fact many-none
        (
            (many (expect-char \a))
            (positional-stream "b")
        )
        :is
        [[[\b 0 1 1] [:eof 1 1 2]] []]
    )
)

(suite "many1"
    (:fact many1-one
        (
            (many1 (expect-char \a)) 
            (positional-stream "ab")
        )
        :is
        [[[\b 1 1 2] [:eof 2 1 3]] [[0 1]]]
    )
    (:fact many1-many
        (
            (many1 (expect-char \a)) 
            (positional-stream "aab")
        )
        :is
        [[[\b 2 1 3] [:eof 3 1 4]] [[0 1] [1 2]]]
    )
    (:fact many1-none
        (fn []
            (
                (many1 (expect-char \a)) 
                (positional-stream "b")
            )
        )
        :throws
        InvalidSyntaxException
    )
)

(suite "chain"
    (:fact chain-match-both
        (
            (chain (expect-char \a) (expect-char \b))
            (positional-stream "abc")
        )
        :is
        [[[\c 2 1 3] [:eof 3 1 4]] [[0 1] [1 2]]]
    )
    (:fact chain-match-one
        (fn []
            (
                (chain (expect-char \a) (expect-char \z))
                (positional-stream "abc")
            )
        )
        :throws
        InvalidSyntaxException
    )
    (:fact chain-match-none
        (fn []
            (
                (chain (expect-char \y) (expect-char \z))
                (positional-stream "abc")
            )
        )
        :throws
        InvalidSyntaxException
    )
)

(suite "between"
    (:fact between-match
        (
            (between (expect-char \{) (expect-char \}) (expect-no-eof))
            (positional-stream "{a{b}}")
        )
        :is
        [[[\} 5 1 6] [:eof 6 1 7]] [[0 1] [4 5] [[1 2] [2 3] [3 4]]]]
    )
    (:fact between-unmatch
        (fn []
            (
                (between (expect-char \{) (expect-char \}) (expect-no-eof))
                (positional-stream "{a{b")
            )
        )
        :throws InvalidSyntaxException
    )
)

(suite "foresee"
    (:fact foresee-match
        (
            (foresee (expect-char \a))
            (positional-stream "a")
        )
        :is
        [[[\a 0 1 1] [:eof 1 1 2]] [0 1]]
    )
    (:fact foresee-unmatch
        (fn []
            (
                (foresee (expect-char \a))
                (positional-stream "b")
            )
        )
        :throws InvalidSyntaxException
    )
)

(suite "extract-string-between"
    (:fact extract-string-between-normal
        (let [start-stream (positional-stream "ab")
                end-stream (next start-stream)
            ]
            (extract-string-between start-stream end-stream)
        )
        :is
        "a"
    )
    (:fact extract-string-between-eof
        (fn []
            (let [start-stream (positional-stream "ab")
                    end-stream (next start-stream)
                ]
                (extract-string-between end-stream start-stream)
            )
        )
        :throws IllegalArgumentException
    )
)
