(ns parser-unittest.parser.sql-2003
    (:use 
        [testing.core :only (suite)]
    )
    (:require
        [utilities.parse :as prs]
        [parser.sql-2003 :as sql]
    )
    (:import
        [utilities.parse InvalidSyntaxException]
    )
)

(defn extract-stream [stream]
    (vec (map first stream))
)

(defn extract-result [result]
    (let [[stream result] result]
        [(extract-stream stream) result]
    )
)

(suite "expect-char-ignore-case"
    (:fact expect-char-ignore-case:a
        (->> "a"
            (prs/str->stream)
            ((sql/expect-char-ignore-case \A))
            (extract-result)
        )
        :is
        [[:eof] \a]
    )
    (:fact expect-char-ignore-case:z
        (->> "z"
            (prs/str->stream)
            ((sql/expect-char-ignore-case \Z))
            (extract-result)
        )
        :is
        [[:eof] \z]
    )
    (:fact expect-char-ignore-case:A
        (->> "A"
            (prs/str->stream)
            ((sql/expect-char-ignore-case \A))
            (extract-result)
        )
        :is
        [[:eof] \A]
    )
    (:fact expect-char-ignore-case:Z
        (->> "Z"
            (prs/str->stream)
            ((sql/expect-char-ignore-case \Z))
            (extract-result)
        )
        :is
        [[:eof] \Z]
    )
    (:fact expect-char-ignore-case:0
        (->> "0"
            (prs/str->stream)
            ((sql/expect-char-ignore-case \0))
            (extract-result)
        )
        :is
        [[:eof] \0]
    )
)

(suite "expect-string-ignore-case"
    (:fact expect-string-ignore-case
        (->> "zzZZ"
            (prs/str->stream)
            ((sql/expect-string-ignore-case "zzzz"))
            (extract-result)
            (first)
        )
        :is
        [:eof]
    )
)

(suite "comment"
    (:fact sql-comment:newline
        (->> "--a\nb"
            (prs/str->stream)
            (sql/sql-comment)
            (extract-result)
        )
        :is
        [[\b :eof] nil]
    )
    (:fact sql-comment:eof
        (->> "--a"
            (prs/str->stream)
            (sql/sql-comment)
            (extract-result)
        )
        :is
        [[:eof] nil]
    )
    (:fact sql-comment:bracketed
        (->> "/*a*/"
            (prs/str->stream)
            (sql/sql-comment)
            (extract-result)
        )
        :is
        [[:eof] nil]
    )
)

(suite "blanks"
    (:fact blank+
        (->> " --a\n"
            (prs/str->stream)
            (sql/blank+)
            (extract-result)
        )
        :is
        [[:eof] nil]
    )
    (:fact blank*
        (->> " --a\n"
            (prs/str->stream)
            (sql/blank*)
            (extract-result)
        )
        :is
        [[:eof] nil]
    )
    (:fact blank+:no-blank
        (fn []
            (->> "a"
                (prs/str->stream)
                (sql/blank+)
            )
        )
        :throws InvalidSyntaxException
    )
    (:fact blank*:no-blank
        (->> "a"
            (prs/str->stream)
            (sql/blank*)
            (extract-result)
        )
        :is
        [[\a :eof] nil]        
    )
)

(suite "null"
    (:fact null
        (->> "NULL"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :null-literal, :value "NULL"}]
    )
)

(suite "numeric literals"
    (:fact numeric-literal:natural:single-digit
        (->> "0"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value "0"}]
    )
    (:fact numeric-literal:natural:multi-digit
        (->> "10"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value "10"}]
    )
    (:fact numeric-literal:decimal:lonely-point
        ; although SQL standard supports such syntax, we won't.
        (->> "0."
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[\. :eof] {:type :numeric-literal, :value "0"}]
    )
    (:fact numeric-literal:decimal:fraction
        (->> "0.1"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value "0.1"}]
    )
    (:fact numeric-literal:decimal:pure-fraction
        (->> ".1"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value ".1"}]
    )
    (:fact numeric-literal:scientific:exp-no-sign
        (->> "1E1"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value "1E1"}]
    )
    (:fact numeric-literal:scientific:small-e
        (->> "1e1"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value "1e1"}]
    )
    (:fact numeric-literal:scientific:exp-plus-sign
        (->> "1E+1"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value "1E+1"}]
    )
    (:fact numeric-literal:scientific:exp-minus-sign
        (->> "1E-1"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value "1E-1"}]
    )
    (:fact numeric-literal:plus-sign
        (->> "+1"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value "+1"}]
    )
    (:fact numeric-literal:minus-sign
        (->> "-1"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :numeric-literal, :value "-1"}]
    )
)

(suite "hex string literal"
    (:fact hex-string-literal:one-segment
        (->> "x'0Af'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :hex-string-literal, :value "x'0Af'"}]
    )
    (:fact hex-string-literal:big-X
        (->> "X'0Af'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :hex-string-literal, :value "X'0Af'"}]
    )
    (:fact hex-string-literal:multi-segment
        (->> "X'0' 'A'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :hex-string-literal, :value "X'0' 'A'"}]
    )
    (:fact hex-string-literal:escape
        ; do not support yet
        (fn []
            (->> "X'0'ESCAPE"
                (prs/str->stream)
                (sql/literal)
            )
        )
        :throws InvalidSyntaxException
    )
)

(suite "datatime literal"
    (:fact date-literal
        (->> "DATE'2013-10-01'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :date-literal, :value "DATE'2013-10-01'"}]
    )
    (:fact time-literal
        (->> "TIME'14:40:30'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :time-literal, :value "TIME'14:40:30'"}]
    )
    (:fact time-literal:second-fraction
        (->> "TIME'14:40:30.456'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :time-literal, :value "TIME'14:40:30.456'"}]
    )
    (:fact time-literal:timezone
        (->> "TIME'14:40:30+08:00'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :time-literal, :value "TIME'14:40:30+08:00'"}]
    )
    (:fact timestamp-literal
        (->> "TIMESTAMP'2013-10-01 14:40:30+08:00'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :timestamp-literal, :value "TIMESTAMP'2013-10-01 14:40:30+08:00'"}]
    )
)

(suite "interval literal"
    (:fact interval:year
        (->> "INTERVAL'30'YEAR"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'30'YEAR"}]
    )
    (:fact interval:plus
        (->> "INTERVAL'+30'YEAR"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'+30'YEAR"}]
    )
    (:fact interval:minus
        (->> "INTERVAL'-30'YEAR"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'-30'YEAR"}]
    )
    (:fact interval:year-month
        (->> "INTERVAL'30-5'MONTH"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'30-5'MONTH"}]
    )
    (:fact interval:month
        (->> "INTERVAL'5'MONTH"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'MONTH"}]
    )
    (:fact interval:day
        (->> "INTERVAL'5'DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'DAY"}]
    )
    (:fact interval:day-hour
        (->> "INTERVAL'5 14'DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5 14'DAY"}]
    )
    (:fact interval:day-hour-minute
        (->> "INTERVAL'5 14:30'DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5 14:30'DAY"}]
    )
    (:fact interval:day-hour-minute-second
        (->> "INTERVAL'5 14:30:21.456'DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5 14:30:21.456'DAY"}]
    )
    (:fact interval:hour-minute-second
        (->> "INTERVAL'14:30:21.456'DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'14:30:21.456'DAY"}]
    )
    (:fact interval:minute-second
        (->> "INTERVAL'30:21.456'DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'30:21.456'DAY"}]
    )
    (:fact interval:second
        (->> "INTERVAL'21.456'DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'21.456'DAY"}]
    )
    (:fact interval:qualifier:single:year
        (->> "INTERVAL'5'YEAR"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'YEAR"}]
    )
    (:fact interval:qualifier:single:month
        (->> "INTERVAL'5'MONTH"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'MONTH"}]
    )
    (:fact interval:qualifier:single:day
        (->> "INTERVAL'5'DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'DAY"}]
    )
    (:fact interval:qualifier:single:hour
        (->> "INTERVAL'5'HOUR"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'HOUR"}]
    )
    (:fact interval:qualifier:single:minute
        (->> "INTERVAL'5'MINUTE"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'MINUTE"}]
    )
    (:fact interval:qualifier:single:minute:precision
        (->> "INTERVAL'5'MINUTE(1)"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'MINUTE(1)"}]
    )
    (:fact interval:qualifier:single:second
        (->> "INTERVAL'5'SECOND"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'SECOND"}]
    )
    (:fact interval:qualifier:single:second:precision
        (->> "INTERVAL'5'SECOND(1)"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'SECOND(1)"}]
    )
    (:fact interval:qualifier:single:second:precision-fraction
        (->> "INTERVAL'5'SECOND(1,1)"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'SECOND(1,1)"}]
    )
    (:fact interval:qualifier:start-to-end
        (->> "INTERVAL'5'DAY TO DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'DAY TO DAY"}]
    )
    (:fact interval:qualifier:start-to-end:start-precision
        (->> "INTERVAL'5'DAY(1)TO DAY"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'DAY(1)TO DAY"}]
    )
    (:fact interval:qualifier:start-to-end:end-second
        (->> "INTERVAL'5'DAY TO SECOND"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'DAY TO SECOND"}]
    )
    (:fact interval:qualifier:start-to-end:end-second-precision
        (->> "INTERVAL'5'DAY TO SECOND(1)"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :interval-literal, :value "INTERVAL'5'DAY TO SECOND(1)"}]
    )
)

(suite "national character string literal"
    (:fact national-string:one-segment
        (->> "N'a'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :national-string-literal, :value "N'a'"}]
    )
    (:fact national-string:escaped-quote
        (->> "N'a''b'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :national-string-literal, :value "N'a''b'"}]
    )
    (:fact national-string:multi-segments
        (->> "N'a' 'b'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :national-string-literal, :value "N'a' 'b'"}]
    )
)

(suite "character string literal"
    ; this is not completely compliant to SQL-92 for lack of leading charactor set spec
    (:fact character-string-literal:one-segment
        (->> "'a'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :character-string-literal, :value "'a'"}]
    )
    (:fact character-string-literal:escaped-quote
        (->> "'a''b'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :character-string-literal, :value "'a''b'"}]
    )
    (:fact character-string-literal:multi-segments
        (->> "'a' 'b'"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :character-string-literal, :value "'a' 'b'"}]
    )
    (:fact character-string-literal:spec
        ; do not support yet
        (fn []
            (->> "_GBK'a'"
                (prs/str->stream)
                (sql/literal)
                (extract-result)
            )
        )
        :throws InvalidSyntaxException
    )
)

(suite "boolean literals"
    (:fact boolean-literal:true
        (->> "TRUE"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :boolean-literal, :value "TRUE"}]
    )
    (:fact boolean-literal:false
        (->> "FALSE"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :boolean-literal, :value "FALSE"}]
    )
    (:fact boolean-literal:unknown
        (->> "UNKNOWN"
            (prs/str->stream)
            (sql/literal)
            (extract-result)
        )
        :is
        [[:eof] {:type :boolean-literal, :value "UNKNOWN"}]
    )
)

(suite "identifier"
    (:fact identifier
        (->> "a"
            (prs/str->stream)
            (sql/identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :identifier, :value "a"}]
    )
    (:fact identifier:underscore
        (->> "_"
            (prs/str->stream)
            (sql/identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :identifier, :value "_"}]
    )
    (:fact identifier:digit
        (fn []
            (->> "0"
                (prs/str->stream)
                (sql/identifier)
                (extract-result)
            )
        )
        :throws InvalidSyntaxException
    )
    (:fact identifier:letter-digit
        (->> "a0"
            (prs/str->stream)
            (sql/identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :identifier, :value "a0"}]
    )
    (:fact identifier:doublequoted
        (->> "\"a\""
            (prs/str->stream)
            (sql/identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :identifier, :value "a"}]
    )
    (:fact identifier:doublequoted:escaped
        (->> "\"a\"\"b\""
            (prs/str->stream)
            (sql/identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :identifier, :value "a\"b"}]
    )
    (:fact identifier:period-in-doublequotes
        (->> "\"a.b\""
            (prs/str->stream)
            (sql/identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :identifier, :value "a.b"}]
    )
    (:fact identifier:backquoted
        (->> "`a`"
            (prs/str->stream)
            (sql/identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :identifier, :value "a"}]
    )
    (:fact identifier:backquoted:escaped
        (->> "`a``b`"
            (prs/str->stream)
            (sql/identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :identifier, :value "a`b"}]
    )
    (:fact identifier:period-in-backquoted
        (->> "`a.b`"
            (prs/str->stream)
            (sql/identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :identifier, :value "a.b"}]
    )
    (:fact identifier:reserved-keyword
        (fn []
            (->> "AS"
                (prs/str->stream)
                (sql/identifier)
            )
        )
        :throws InvalidSyntaxException
    )
)

(suite "dotted identifier"
    (:fact dotted-identifier
        (->> "a.b"
            (prs/str->stream)
            (sql/dotted-identifier)
            (extract-result)
        )
        :is
        [[:eof] {:type :dotted-identifier, :value ["a" "b"]}]
    )
)

(suite "column name list"
    (:fact column-list:single
        (->> "a"
            (prs/str->stream)
            (sql/column-list)
            (extract-result)
        )
        :is
        [[:eof] {:type :column-list, :value ["a"]}]
    )
    (:fact column-list:multi
        (->> "a, b"
            (prs/str->stream)
            (sql/column-list)
            (extract-result)
        )
        :is
        [[:eof] {:type :column-list, :value ["a" "b"]}]
    )
    (:fact paren-column-list
        (->> "(a, b)"
            (prs/str->stream)
            (sql/paren-column-list)
            (extract-result)
        )
        :is
        [[:eof] {:type :column-list, :value ["a" "b"]}]
    )
)

(suite "correlation"
    (:fact correlation:as
        (->> "AS tbl"
            (prs/str->stream)
            (sql/table-correlation)
            (extract-result)
        )
        :is
        [[:eof] {:type :table-correlation, :name "tbl"}]
    )
    (:fact correlation:no-as
        (->> "tbl"
            (prs/str->stream)
            (sql/table-correlation)
            (extract-result)
        )
        :is
        [[:eof] {:type :table-correlation, :name "tbl"}]
    )
)

(suite "table correlation"
    (:fact table-correlation:column-list
        (->> "AS tbl(a, b)"
            (prs/str->stream)
            (sql/table-correlation)
            (extract-result)
        )
        :is
        [[:eof] {:type :table-correlation, :name "tbl", :column-list ["a" "b"]}]
    )
)

(suite "from clause"
    (:fact from-clause:table-name
        (->> "FROM tbl"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :table, :refer ["tbl"]}
        ]}]
    )
    (:fact from-clause:table-name:dotted
        (->> "FROM catalog.tbl"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :table, :refer ["catalog" "tbl"]}
        ]}]
    )
    (:fact from-clause:table-name:as
        (->> "FROM catalog.tbl AS a"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :table, :name "a", :refer ["catalog" "tbl"]}
        ]}]
    )
    (:fact from-clause:paren
        (->> "FROM (catalog.tbl AS a)"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :table, :name "a", :refer ["catalog" "tbl"]}
        ]}]
    )
    (:fact from-clause:table-name:as:column-list
        (->> "FROM catalog.tbl AS a(b)"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :table, :name "a", :refer ["catalog" "tbl"], :column-list ["b"]}
        ]}]
    )
    (:fact from-clause:table-name:multiple
        (->> "FROM tbl1, tbl2"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :table, :refer ["tbl1"]}
            {:type :table, :refer ["tbl2"]}
        ]}]
    )
    (:fact from-clause:cross-join
        (->> "FROM tbl1 CROSS JOIN tbl2"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :cross-join, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
    (:fact from-clause:cross-join:on
        (->> "FROM tbl1 CROSS JOIN tbl2 ON tbl1.col=tbl2.col"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :cross-join, 
                :on {
                    :type :=
                    :left {:type :dotted-identifier, :value ["tbl1" "col"]}
                    :right {:type :dotted-identifier, :value ["tbl2" "col"]}
                }
                :left {:type :table, :refer ["tbl1"]}, 
                :right {:type :table, :refer ["tbl2"]}
            }
        ]}]
    )
    (:fact from-clause:inner-join
        (->> "FROM tbl1 JOIN tbl2"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :join, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
    (:fact from-clause:inner-join:on
        (->> "FROM tbl1 JOIN tbl2 ON TRUE"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :join, :on {:type :boolean-literal, :value "TRUE"}, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
    (:fact from-clause:outer-join:left
        (->> "FROM tbl1 LEFT JOIN tbl2 ON TRUE"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :outer-join, :join-type :left, :on {:type :boolean-literal, :value "TRUE"}, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
    (:fact from-clause:outer-join:left-outer
        (->> "FROM tbl1 LEFT JOIN tbl2 ON TRUE"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :outer-join, :join-type :left, :on {:type :boolean-literal, :value "TRUE"}, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
    (:fact from-clause:outer-join:right
        (->> "FROM tbl1 RIGHT JOIN tbl2 ON TRUE"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :outer-join, :join-type :right, :on {:type :boolean-literal, :value "TRUE"}, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
    (:fact from-clause:outer-join:right-outer
        (->> "FROM tbl1 RIGHT JOIN tbl2 ON TRUE"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :outer-join, :join-type :right, :on {:type :boolean-literal, :value "TRUE"}, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
    (:fact from-clause:outer-join:full
        (->> "FROM tbl1 FULL JOIN tbl2 ON TRUE"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :outer-join, :join-type :full, :on {:type :boolean-literal, :value "TRUE"}, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
    (:fact from-clause:outer-join:full-outer
        (->> "FROM tbl1 FULL OUTER JOIN tbl2 ON TRUE"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :outer-join, :join-type :full, :on {:type :boolean-literal, :value "TRUE"}, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
    (:fact from-clause:join-after-join
        (->> "FROM tbl1 JOIN tbl2 ON TRUE JOIN tbl3 ON FALSE"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {
                :type :join
                :on {:type :boolean-literal, :value "FALSE"}
                :left {
                    :type :join
                    :on {:type :boolean-literal, :value "TRUE"}
                    :left {
                        :type :table, :refer ["tbl1"]
                    }
                    :right {
                        :type :table, :refer ["tbl2"]
                    }
                }
                :right {
                    :type :table, :refer ["tbl3"]
                }
            }
        ]}]
    )
    (:fact from-clause:join:paren
        (->> "FROM (tbl1 JOIN tbl2)"
            (prs/str->stream)
            (sql/from-clause)
            (extract-result)
        )
        :is
        [[:eof] {:type :from-clause, :tables [
            {:type :join, :left {:type :table, :refer ["tbl1"]}, :right {:type :table, :refer ["tbl2"]}}
        ]}]
    )
)

(suite "query"
    (:fact select
        (->> "SELECT * FROM tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select, 
            :select-list [{:type :derived-column, :value {:type :asterisk}}],
            :from-clause [{:type :table, :refer ["tbl"]}]
        }]
    )
    (:fact select:set-quantifier:distinct
        (->> "SELECT DISTINCT * FROM tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select, :set-quantifier :distinct
            :select-list [{:type :derived-column, :value {:type :asterisk}}]
            :from-clause [{:type :table, :refer ["tbl"]}]
        }]
    )
    (:fact select:set-quantifier:all
        (->> "SELECT ALL * FROM tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select, :set-quantifier :all
            :select-list [{:type :derived-column, :value {:type :asterisk}}]
            :from-clause [{:type :table, :refer ["tbl"]}]
        }]
    )
    (:fact select:asterisked-identifier-chain
        (->> "SELECT ns.tbl.* FROM ns.tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :from-clause [{:type :table, :refer ["ns" "tbl"]}]
            :select-list [{:type :derived-column, :value {:type :asterisk, :refer ["ns" "tbl"]}}]
        }]
    )
    (:fact select:asterisked-identifier-chain:as
        (->> "SELECT tbl.* AS (a) FROM tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :from-clause [{:type :table, :refer ["tbl"]}]
            :select-list [{
                :type :derived-column, 
                :value {:type :asterisk, :refer ["tbl"]}
                :name [
                    {:type :identifier, :value "a"}
                ]
            }]
        }]
    )
    (:fact select:derived-column
        (->> "SELECT 1 FROM ns.tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :from-clause [{:type :table, :refer ["ns" "tbl"]}]
            :select-list [
                {
                    :type :derived-column, :value {:type :numeric-literal, :value "1"}
                }
            ]
        }]
    )
    (:fact select:derived-column:as
        (->> "SELECT 1 AS a FROM ns.tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :from-clause [{:type :table, :refer ["ns" "tbl"]}]
            :select-list [{
                :type :derived-column
                :name {:type :identifier, :value "a"}
                :value {:type :numeric-literal, :value "1"}
            }]
        }]
    )
    (:fact select:derived-column:as:no-as
        (->> "SELECT 1 a FROM ns.tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :from-clause [{:type :table, :refer ["ns" "tbl"]}]
            :select-list [{
                :type :derived-column
                :name {:type :identifier, :value "a"}
                :value {:type :numeric-literal, :value "1"}
            }]
        }]
    )
    (:fact select:direct-column
        (->> "SELECT col FROM tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :from-clause [{:type :table, :refer ["tbl"]}]
            :select-list [
                {
                    :type :derived-column
                    :value {:type :dotted-identifier, :value ["col"]}
                }
            ]
        }]
    )
    (:fact select:where-clause
        (->> "SELECT * FROM tbl WHERE col<2"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :select-list [{:type :derived-column, :value {:type :asterisk}}]
            :from-clause [{:type :table, :refer ["tbl"]}]
            :where {:type :<
                :left {:type :dotted-identifier, :value ["col"]}
                :right {:type :numeric-literal, :value "2"}
            }
        }]
    )
    (:fact select:group-by
        (->> "SELECT * FROM tbl GROUP BY col, 1"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :select-list [{:type :derived-column, :value {:type :asterisk}}]
            :from-clause [{:type :table, :refer ["tbl"]}]
            :group-by [
                {:type :identifier, :value "col"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact select:order-by
        (->> "SELECT * FROM tbl ORDER BY col, col ASC, 1 DESC"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :select-list [{:type :derived-column, :value {:type :asterisk}}]
            :from-clause [{:type :table, :refer ["tbl"]}]
            :order-by [
                {:ordering nil, :value {:type :identifier, :value "col"}}
                {:ordering :asc, :value {:type :identifier, :value "col"}}
                {:ordering :desc, :value {:type :numeric-literal, :value "1"}}
            ]
        }]
    )
    (:fact select:limit
        (->> "SELECT * FROM tbl LIMIT 1"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :select-list [{:type :derived-column, :value {:type :asterisk}}]
            :from-clause [{:type :table, :refer ["tbl"]}]
            :limit {:type :numeric-literal, :value "1"}
        }]
    )
    (:fact union:all
        (->> "SELECT * FROM tbl UNION ALL SELECT * FROM tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :union, :qualifier :all,
            :selects [{
                :type :select
                :select-list [{:type :derived-column, :value {:type :asterisk}}]
                :from-clause [{:type :table, :refer ["tbl"]}]
            } {
                :type :select
                :select-list [{:type :derived-column, :value {:type :asterisk}}]
                :from-clause [{:type :table, :refer ["tbl"]}]
            }]
        }]
    )
    (:fact union:all:paren
        (->> "(SELECT * FROM tbl) UNION ALL (SELECT * FROM tbl)"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :union, :qualifier :all,
            :selects [{
                :type :select
                :select-list [{:type :derived-column, :value {:type :asterisk}}]
                :from-clause [{:type :table, :refer ["tbl"]}]
            } {
                :type :select
                :select-list [{:type :derived-column, :value {:type :asterisk}}]
                :from-clause [{:type :table, :refer ["tbl"]}]
            }]
        }]
    )
)

(suite "expression"
    (:fact expr
        (->> "TRUE OR NOT FALSE AND UNKNOWN"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :value-expr, :value [
            {:type :boolean-literal, :value "TRUE"}
            :or
            {
                :type :boolean-negation
                :value {:type :boolean-literal, :value "FALSE"}
            }
            :and
            {:type :boolean-literal, :value "UNKNOWN"}
        ]}]
    )
    (:fact expr:negation:paren
        (->> "NOT (FALSE AND TRUE)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :boolean-negation, :value {
            :type :value-expr, :value [
                {:type :boolean-literal, :value "FALSE"}
                :and
                {:type :boolean-literal, :value "TRUE"}
            ]}
        }]
    )
    (:fact expr:negation:exclamation
        (->> "!TRUE"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :boolean-negation, :value {
            :type :boolean-literal, :value "TRUE"
        }}]
    )
    (:fact expr:boolean-and:&&
        (->> "TRUE && FALSE"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :value-expr, :value [
            {:type :boolean-literal, :value "TRUE"}
            :and
            {:type :boolean-literal, :value "FALSE"}
        ]}]
    )
    (:fact expr:boolean-or:||
        (->> "TRUE || FALSE"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :value-expr, :value [
            {:type :boolean-literal, :value "TRUE"}
            :or
            {:type :boolean-literal, :value "FALSE"}
        ]}]
    )
    (:fact expr:paren
        (->> "(FALSE AND TRUE) AND (UNKNOWN)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :value-expr, :value [
            {:type :value-expr, :value [
                {:type :boolean-literal, :value "FALSE"}
                :and
                {:type :boolean-literal, :value "TRUE"}
            ]}
            :and
            {:type :boolean-literal, :value "UNKNOWN"}
        ]}]
    )
    (:fact expr:test
        (->> "TRUE IS TRUE"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {
            :type :is
            :left {:type :boolean-literal, :value "TRUE"}
            :right {:type :boolean-literal, :value "TRUE"}
        }]
    )
    (:fact expr:test:not
        (->> "TRUE IS NOT FALSE"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {
            :type :is-not
            :left {:type :boolean-literal, :value "TRUE"}
            :right {:type :boolean-literal, :value "FALSE"}
        }]
    )
    (:fact expr:test:null
        (->> "NULL IS NULL"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {
            :type :is
            :left {:type :null-literal, :value "NULL"}
            :right {:type :null-literal, :value "NULL"}
        }]
    )
    (:fact expr:test:null:not
        (->> "TRUE IS NOT NULL"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {
            :type :is-not
            :left {:type :boolean-literal, :value "TRUE"}
            :right {:type :null-literal, :value "NULL"}
        }]
    )
    (:fact expr:test:chain
        (->> "TRUE IS NULL IS NOT FALSE"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {
            :type :is-not
            :left {
                :type :is
                :left {:type :boolean-literal, :value "TRUE"}
                :right {:type :null-literal, :value "NULL"}
            }
            :right {:type :boolean-literal, :value "FALSE"}
        }]
    )
    (:fact expr:test:chain:precedence-error
        (fn []
            (->> "TRUE IS FALSE IS NOT NULL"
                (prs/str->stream)
                ((prs/chain 
                    sql/value-expr
                    (prs/expect-eof)
                ))
            )
        )
        :throws InvalidSyntaxException
    )
    (:fact expr:negation
        (->> "NOT TRUE"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {
            :type :boolean-negation, 
            :value {:type :boolean-literal, :value "TRUE"}
        }]
    )
    (:fact expr:comparison:<
        (->> "1<2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :<
            :left {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "2"}
        }]
    )
    (:fact expr:comparison:>
        (->> "1> 2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :>
            :left {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "2"}
        }]
    )
    (:fact expr:comparison:=
        (->> "1 =2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :=
            :left {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "2"}
        }]
    )
    (:fact expr:comparison:<>
        (->> "1 <> 2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :<>
            :left {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "2"}
        }]
    )
    (:fact expr:comparison:!=
        (->> "1 != 2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :<>
            :left {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "2"}
        }]
    )
    (:fact expr:comparison:<=>
        (->> "1 <=> 2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :<=>
            :left {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "2"}
        }]
    )
    (:fact expr:comparison:<=
        (->> "1<=2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :<=
            :left {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "2"}
        }]
    )
    (:fact expr:comparison:>=
        (->> "1>=2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :>=
            :left {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "2"}
        }]
    )
    (:fact expr:predicate:in-array
        (->> "1 IN (1, 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :in-array
            :left {:type :numeric-literal, :value "1"}
            :right [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
    (:fact expr:predicate:in-array:not
        (->> "3 NOT IN (1, 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :not-in-array
            :left {:type :numeric-literal, :value "3"}
            :right [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
    (:fact expr:predicate:between
        (->> "2 BETWEEN 1 AND 3"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :between
            :left {:type :numeric-literal, :value "2"}
            :middle {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "3"}
        }]
    )
    (:fact expr:predicate:between:not
        (->> "0 NOT BETWEEN 1 AND 3"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :not-between
            :left {:type :numeric-literal, :value "0"}
            :middle {:type :numeric-literal, :value "1"}
            :right {:type :numeric-literal, :value "3"}
        }]
    )
    (:fact expr:predicate:like
        (->> "'a' LIKE 'a'"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :like
            :left {:type :character-string-literal, :value "'a'"}
            :right {:type :character-string-literal, :value "'a'"}
        }]
    )
    (:fact expr:predicate:like:not
        (->> "'a' NOT LIKE 'b'"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :not-like
            :left {:type :character-string-literal, :value "'a'"}
            :right {:type :character-string-literal, :value "'b'"}
        }]
    )
    (:fact expr:predicate:reglike
        (->> "'a' REGEXP 'a'"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :reglike
            :left {:type :character-string-literal, :value "'a'"}
            :right {:type :character-string-literal, :value "'a'"}
        }]
    )
    (:fact expr:predicate:reglike:not
        (->> "'a' NOT REGEXP 'b'"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :not-reglike
            :left {:type :character-string-literal, :value "'a'"}
            :right {:type :character-string-literal, :value "'b'"}
        }]
    )
    (:fact expr:bit:|
        (->> "1|2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :|
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:bit:&
        (->> "1&2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :&
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:bit:<<
        (->> "1<<2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :<<
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:bit:>>
        (->> "1>>2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :>>
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:arithmetic:+
        (->> "1+2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :+
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:arithmetic:-
        (->> "1-2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :-
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:arithmetic:*
        (->> "1*2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :*
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:arithmetic:div
        (->> "1/2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :div
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:arithmetic:mod
        (->> "1%2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :mod
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:bit:caret
        (->> "1^2"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :bit-expr, :value [
            {:type :numeric-literal, :value "1"}
            :caret
            {:type :numeric-literal, :value "2"}
        ]}]
    )
    (:fact expr:arithmetic:unary+
        (->> "+ 1"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :unary+
            :value {:type :numeric-literal, :value "1"}
        }]
    )
    (:fact expr:arithmetic:unary-
        (->> "- 1"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :unary-
            :value {:type :numeric-literal, :value "1"}
        }]
    )
    (:fact expr:bit:unary-tilde
        (->> "~ 1"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :unary-tilde
            :value {:type :numeric-literal, :value "1"}
        }]
    )
    (:fact expr:binary
        (->> "BINARY 'a'"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :binary
            :value {:type :character-string-literal, :value "'a'"}
        }]
    )
    (:fact expr:subquery
        (->> "(SELECT * FROM tbl)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :select-list [{:type :derived-column, :value {:type :asterisk}}],
            :from-clause [{:type :table, :refer ["tbl"]}]
        }]
    )
    (:fact expr:exists
        (->> "EXISTS (SELECT * FROM tbl)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :exists, :value {
            :type :select
            :select-list [{:type :derived-column, :value {:type :asterisk}}],
            :from-clause [{:type :table, :refer ["tbl"]}]
        }}]
    )
    (:fact expr:identifier
        (->> "tbl.col"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :dotted-identifier, :value ["tbl" "col"]}]
    )
    (:fact expr:identifier:asterisk
        (->> "*"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :asterisk}]
    )
    (:fact expr:identifier:asterisk:table
        (->> "tbl.*"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :asterisk, :refer ["tbl"]}]
    )
    (:fact expr:case
        (->> "CASE WHEN TRUE THEN 1 END"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :case
            :when [
                [{:type :boolean-literal, :value "TRUE"} {:type :numeric-literal, :value "1"}]
            ]
        }]
    )
    (:fact expr:case:else
        (->> "CASE WHEN FALSE THEN 1 ELSE 2 END"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :case
            :when [
                [{:type :boolean-literal, :value "FALSE"} {:type :numeric-literal, :value "1"}]
            ]
            :else {:type :numeric-literal, :value "2"}
        }]
    )
    (:fact expr:case:value
        (->> "CASE 1 WHEN 1 THEN TRUE WHEN 0 THEN FALSE END"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :case
            :value {:type :numeric-literal, :value "1"}
            :when [
                [{:type :numeric-literal, :value "1"} {:type :boolean-literal, :value "TRUE"}]
                [{:type :numeric-literal, :value "0"} {:type :boolean-literal, :value "FALSE"}]
            ]
        }]
    )
)

(suite "functions"
    (:fact func:cast:int
        (->> "CAST('1' AS INT)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :int
        }]
    )
    (:fact func:cast:tinyint
        (->> "CAST('1' AS TINYINT)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :tinyint
        }]
    )
    (:fact func:cast:smallint
        (->> "CAST('1' AS SMALLINT)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :smallint
        }]
    )
    (:fact func:cast:bigint
        (->> "CAST('1' AS BIGINT)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :bigint
        }]
    )
    (:fact func:cast:float
        (->> "CAST('1' AS FLOAT)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :float
        }]
    )
    (:fact func:cast:double
        (->> "CAST('1' AS DOUBLE)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :double
        }]
    )
    (:fact func:cast:decimal
        (->> "CAST('1' AS DECIMAL)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :decimal
        }]
    )
    (:fact func:cast:timestamp
        (->> "CAST('1' AS TIMESTAMP)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :timestamp
        }]
    )
    (:fact func:cast:date
        (->> "CAST('1' AS DATE)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :date
        }]
    )
    (:fact func:cast:string
        (->> "CAST('1' AS STRING)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :string
        }]
    )
    (:fact func:cast:varchar
        (->> "CAST('1' AS varchar)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :varchar
        }]
    )
    (:fact func:cast:boolean
        (->> "CAST('1' AS BOOLEAN)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :boolean
        }]
    )
    (:fact func:cast:binary
        (->> "CAST('1' AS BINARY)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :cast
            :left {:type :character-string-literal, :value "'1'"}
            :right :binary
        }]
    )
    (:fact func:aggregate:distinct-count:1
        (->> "COUNT(DISTINCT 0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :distinct-count
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:aggregate:distinct-count:2
        (->> "COUNT(DISTINCT 0, 1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :distinct-count
            :args [
                {:type :numeric-literal, :value "0"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:distinct-count:0
        (fn []
            (->> "COUNT(DISTINCT)"
                (prs/str->stream)
                ((prs/chain sql/value-expr (prs/expect-eof)))
            )
        )
        :throws InvalidSyntaxException
    )
    (:fact func:aggregate:distinct-count:3
        (fn []
            (->> "COUNT(DISTINCT 0,1,2)"
                (prs/str->stream)
                ((prs/chain sql/value-expr (prs/expect-eof)))
            )
        )
        :throws InvalidSyntaxException
    )
    (:fact func:aggregate:distinct-sum:1
        (->> "SUM(DISTINCT 0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :distinct-sum
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:aggregate:distinct-avg:1
        (->> "AVG(DISTINCT 0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :distinct-avg
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:pow:2
        (->> "POW(0, 1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :power
            :args [
                {:type :numeric-literal, :value "0"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:pow:1
        (fn []
            (->> "POW(0)"
                (prs/str->stream)
                ((prs/chain sql/value-expr (prs/expect-eof)))
                (extract-result)
            )
        )
        :throws InvalidSyntaxException
    )
    (:fact func:math:pow:3
        (fn []
            (->> "POW(0, 1, 2)"
                (prs/str->stream)
                ((prs/chain sql/value-expr (prs/expect-eof)))
                (extract-result)
            )
        )
        :throws InvalidSyntaxException
    )
    (:fact func:math:power
        (->> "POWER(0, 1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :power
            :args [
                {:type :numeric-literal, :value "0"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:round:1
        (->> "ROUND(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :round
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:round:2
        (->> "ROUND(0, 1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :round
            :args [
                {:type :numeric-literal, :value "0"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:floor:1
        (->> "FLOOR(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :floor
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:ceil:1
        (->> "CEIL(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :ceil
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:ceiling:1
        (->> "CEILING(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :ceil
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:rand:0
        (->> "RAND()"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :rand
            :args [
            ]
        }]
    )
    (:fact func:math:rand:1
        (->> "RAND(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :rand
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:exp:1
        (->> "EXP(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :exp
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:ln:1
        (->> "LN(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :ln
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:log10:1
        (->> "LOG10(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :log10
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:log2:1
        (->> "LOG2(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :log2
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:log:2
        (->> "LOG(1, 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :log
            :args [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
    (:fact func:math:sqrt:1
        (->> "SQRT(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :sqrt
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:bin:1
        (->> "BIN(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :bin
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:hex:1
        (->> "HEX(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :hex
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:unhex:1
        (->> "UNHEX('1')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :unhex
            :args [
                {:type :character-string-literal, :value "'1'"}
            ]
        }]
    )
    (:fact func:math:conv:3
        (->> "CONV(1, 2, 3)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :conv
            :args [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
                {:type :numeric-literal, :value "3"}
            ]
        }]
    )
    (:fact func:math:abs:1
        (->> "ABS(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :abs
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:pmod:2
        (->> "PMOD(1, 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :pmod
            :args [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
    (:fact func:math:sin:1
        (->> "SIN(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :sin
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:asin:1
        (->> "ASIN(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :asin
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:cos:1
        (->> "COS(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :cos
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:acos:1
        (->> "ACOS(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :acos
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:math:tan:1
        (->> "TAN(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :tan
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:atan:1
        (->> "ATAN(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :atan
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:degrees:1
        (->> "DEGREES(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :degrees
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:radians:1
        (->> "RADIANS(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :radians
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:positive:1
        (->> "POSITIVE(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :positive
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:negative:1
        (->> "NEGATIVE(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :negative
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:sign:1
        (->> "SIGN(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :sign
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:math:e:0
        (->> "E()"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :e
            :args [
            ]
        }]
    )
    (:fact func:math:pi:0
        (->> "PI()"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :pi
            :args [
            ]
        }]
    )
    (:fact func:conversion:binary:1
        (->> "BINARY(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :binary
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:date:from_unixtime:1
        (->> "FROM_UNIXTIME(0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :from_unixtime
            :args [
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:date:from_unixtime:2
        (->> "FROM_UNIXTIME(0, 'YYYY')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :from_unixtime
            :args [
                {:type :numeric-literal, :value "0"}
                {:type :character-string-literal, :value "'YYYY'"}
            ]
        }]
    )
    (:fact func:date:unix_timestamp:0
        (->> "UNIX_TIMESTAMP()"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :unix_timestamp
            :args [
            ]
        }]
    )
    (:fact func:date:unix_timestamp:1
        (->> "UNIX_TIMESTAMP('2013')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :unix_timestamp
            :args [
                {:type :character-string-literal, :value "'2013'"}
            ]
        }]
    )
    (:fact func:date:unix_timestamp:2
        (->> "UNIX_TIMESTAMP('2013', 'YYYY')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :unix_timestamp
            :args [
                {:type :character-string-literal, :value "'2013'"}
                {:type :character-string-literal, :value "'YYYY'"}
            ]
        }]
    )
    (:fact func:date:to_date:1
        (->> "TO_DATE('1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :to_date
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:year:1
        (->> "YEAR('1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :year
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:month:1
        (->> "MONTH('1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :month
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:day:1
        (->> "DAY('1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :day
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:dayofmonth:1
        (->> "DAYOFMONTH('1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :day
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:hour:1
        (->> "HOUR('1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :hour
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:minute:1
        (->> "MINUTE('1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :minute
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:second:1
        (->> "SECOND('1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :second
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:weekofyear:1
        (->> "WEEKOFYEAR('1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :weekofyear
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:datediff:2
        (->> "DATEDIFF('1970-01-02', '1970-01-01')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :datediff
            :args [
                {:type :character-string-literal, :value "'1970-01-02'"}
                {:type :character-string-literal, :value "'1970-01-01'"}
            ]
        }]
    )
    (:fact func:date:date_add:2
        (->> "DATE_ADD('1970-01-01', 1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :date_add
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:date:date_sub:2
        (->> "DATE_SUB('1970-01-01', 1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :date_sub
            :args [
                {:type :character-string-literal, :value "'1970-01-01'"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:date:from_utc_timestamp:2
        (->> "FROM_UTC_TIMESTAMP(0, 'CST')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :from_utc_timestamp
            :args [
                {:type :numeric-literal, :value "0"}
                {:type :character-string-literal, :value "'CST'"}
            ]
        }]
    )
    (:fact func:date:to_utc_timestamp:2
        (->> "TO_UTC_TIMESTAMP(0, 'CST')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :to_utc_timestamp
            :args [
                {:type :numeric-literal, :value "0"}
                {:type :character-string-literal, :value "'CST'"}
            ]
        }]
    )
    (:fact func:condition:if:3
        (->> "IF(TRUE, 1, 0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :if
            :args [
                {:type :boolean-literal, :value "TRUE"}
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:str:ascii:1
        (->> "ASCII('0')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :ascii
            :args [
                {:type :character-string-literal, :value "'0'"}
            ]
        }]
    )
    (:fact func:str:base64:1
        (->> "BASE64('0')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :base64
            :args [
                {:type :character-string-literal, :value "'0'"}
            ]
        }]
    )
    (:fact func:str:concat:0
        (->> "CONCAT()"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :concat
            :args [
            ]
        }]
    )
    (:fact func:str:concat:1
        (->> "CONCAT('a')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :concat
            :args [
                {:type :character-string-literal, :value "'a'"}
            ]
        }]
    )
    (:fact func:str:concat:2
        (->> "CONCAT('a', 'b')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :concat
            :args [
                {:type :character-string-literal, :value "'a'"}
                {:type :character-string-literal, :value "'b'"}
            ]
        }]
    )
    (:fact func:str:concat_ws:0
        (fn []
            (->> "CONCAT_WS()"
                (prs/str->stream)
                ((prs/chain sql/value-expr (prs/expect-eof)))
            )
        )
        :throws InvalidSyntaxException
    )
    (:fact func:str:concat_ws:1
        (->> "CONCAT_WS('sep')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :concat_ws
            :args [
                {:type :character-string-literal, :value "'sep'"}
            ]
        }]
    )
    (:fact func:str:concat_ws:2
        (->> "CONCAT_WS('sep', 'a')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :concat_ws
            :args [
                {:type :character-string-literal, :value "'sep'"}
                {:type :character-string-literal, :value "'a'"}
            ]
        }]
    )
    (:fact func:str:decode:2
        (->> "DECODE('a', 'UTF-8')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :decode
            :args [
                {:type :character-string-literal, :value "'a'"}
                {:type :character-string-literal, :value "'UTF-8'"}
            ]
        }]
    )
    (:fact func:str:encode:2
        (->> "ENCODE('a', 'UTF-8')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :encode
            :args [
                {:type :character-string-literal, :value "'a'"}
                {:type :character-string-literal, :value "'UTF-8'"}
            ]
        }]
    )
    (:fact func:str:find_in_set:2
        (->> "FIND_IN_SET('a', 'a,b')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :find_in_set
            :args [
                {:type :character-string-literal, :value "'a'"}
                {:type :character-string-literal, :value "'a,b'"}
            ]
        }]
    )
    (:fact func:str:format_number:2
        (->> "FORMAT_NUMBER(10, 16)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :format_number
            :args [
                {:type :numeric-literal, :value "10"}
                {:type :numeric-literal, :value "16"}
            ]
        }]
    )
    (:fact func:str:get_json_object:2
        (->> "GET_JSON_OBJECT('{\"key\": \"value\"}', 'key')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :get_json_object
            :args [
                {:type :character-string-literal, :value "'{\"key\": \"value\"}'"}
                {:type :character-string-literal, :value "'key'"}
            ]
        }]
    )
    (:fact func:str:in_file:2
        (->> "IN_FILE('a', 'fn')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :in_file
            :args [
                {:type :character-string-literal, :value "'a'"}
                {:type :character-string-literal, :value "'fn'"}
            ]
        }]
    )
    (:fact func:str:instr:2
        (->> "INSTR('abcd', 'bc')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :instr
            :args [
                {:type :character-string-literal, :value "'abcd'"}
                {:type :character-string-literal, :value "'bc'"}
            ]
        }]
    )
    (:fact func:str:length:1
        (->> "LENGTH('abcd')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :length
            :args [
                {:type :character-string-literal, :value "'abcd'"}
            ]
        }]
    )
    (:fact func:str:locate:2
        (->> "LOCATE('bc', 'abcd')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :locate
            :args [
                {:type :character-string-literal, :value "'bc'"}
                {:type :character-string-literal, :value "'abcd'"}
            ]
        }]
    )
    (:fact func:str:locate:3
        (->> "LOCATE('bc', 'abcd', 1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :locate
            :args [
                {:type :character-string-literal, :value "'bc'"}
                {:type :character-string-literal, :value "'abcd'"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:str:lower:1
        (->> "LOWER('bc')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :lower
            :args [
                {:type :character-string-literal, :value "'bc'"}
            ]
        }]
    )
    (:fact func:str:lcase:1
        (->> "LCASE('bc')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :lower
            :args [
                {:type :character-string-literal, :value "'bc'"}
            ]
        }]
    )
    (:fact func:str:lpad:3
        (->> "LPAD('0', 3, '1')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :lpad
            :args [
                {:type :character-string-literal, :value "'0'"}
                {:type :numeric-literal, :value "3"}
                {:type :character-string-literal, :value "'1'"}
            ]
        }]
    )
    (:fact func:str:ltrim:1
        (->> "ltrim('0')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :ltrim
            :args [
                {:type :character-string-literal, :value "'0'"}
            ]
        }]
    )
    (:fact func:str:parse_url:2
        (->> "PARSE_URL('url', 'QUERY')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :parse_url
            :args [
                {:type :character-string-literal, :value "'url'"}
                {:type :character-string-literal, :value "'QUERY'"}
            ]
        }]
    )
    (:fact func:str:parse_url:3
        (->> "PARSE_URL('url', 'QUERY', 'key')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :parse_url
            :args [
                {:type :character-string-literal, :value "'url'"}
                {:type :character-string-literal, :value "'QUERY'"}
                {:type :character-string-literal, :value "'key'"}
            ]
        }]
    )
    (:fact func:str:printf:1
        (->> "PRINTF('a')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :printf
            :args [
                {:type :character-string-literal, :value "'a'"}
            ]
        }]
    )
    (:fact func:str:printf:2
        (->> "PRINTF('a%s', 'b')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :printf
            :args [
                {:type :character-string-literal, :value "'a%s'"}
                {:type :character-string-literal, :value "'b'"}
            ]
        }]
    )
    (:fact func:str:regexp_extract:3
        (->> "REGEXP_EXTRACT('ab', 'b', 0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :regexp_extract
            :args [
                {:type :character-string-literal, :value "'ab'"}
                {:type :character-string-literal, :value "'b'"}
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:str:regexp_replace:3
        (->> "REGEXP_REPLACE('ab', 'b', 'c')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :regexp_replace
            :args [
                {:type :character-string-literal, :value "'ab'"}
                {:type :character-string-literal, :value "'b'"}
                {:type :character-string-literal, :value "'c'"}
            ]
        }]
    )
    (:fact func:str:repeat:2
        (->> "REPEAT('ab', 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :repeat
            :args [
                {:type :character-string-literal, :value "'ab'"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
    (:fact func:str:reverse:1
        (->> "REVERSE('ab')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :reverse
            :args [
                {:type :character-string-literal, :value "'ab'"}
            ]
        }]
    )
    (:fact func:str:rpad:3
        (->> "RPAD('1', 2, '0')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :rpad
            :args [
                {:type :character-string-literal, :value "'1'"}
                {:type :numeric-literal, :value "2"}
                {:type :character-string-literal, :value "'0'"}
            ]
        }]
    )
    (:fact func:str:rtrim:1
        (->> "RTRIM('1')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :rtrim
            :args [
                {:type :character-string-literal, :value "'1'"}
            ]
        }]
    )
    (:fact func:str:space:1
        (->> "SPACE(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :space
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:str:substr:2
        (->> "SUBSTR('a', 0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :substring
            :args [
                {:type :character-string-literal, :value "'a'"}
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:str:substr:3
        (->> "SUBSTR('a', 0, 1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :substring
            :args [
                {:type :character-string-literal, :value "'a'"}
                {:type :numeric-literal, :value "0"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:str:substring:2
        (->> "SUBSTRING('a', 0)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :substring
            :args [
                {:type :character-string-literal, :value "'a'"}
                {:type :numeric-literal, :value "0"}
            ]
        }]
    )
    (:fact func:str:substring:3
        (->> "SUBSTRING('a', 0, 1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :substring
            :args [
                {:type :character-string-literal, :value "'a'"}
                {:type :numeric-literal, :value "0"}
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:str:translate:3
        (->> "TRANSLATE('ab', 'b', 'c')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :translate
            :args [
                {:type :character-string-literal, :value "'ab'"}
                {:type :character-string-literal, :value "'b'"}
                {:type :character-string-literal, :value "'c'"}
            ]
        }]
    )
    (:fact func:str:trim:1
        (->> "TRIM('ab')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :trim
            :args [
                {:type :character-string-literal, :value "'ab'"}
            ]
        }]
    )
    (:fact func:str:unbase64:1
        (->> "UNBASE64('ab')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :unbase64
            :args [
                {:type :character-string-literal, :value "'ab'"}
            ]
        }]
    )
    (:fact func:str:upper:1
        (->> "UPPER('ab')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :upper
            :args [
                {:type :character-string-literal, :value "'ab'"}
            ]
        }]
    )
    (:fact func:str:ucase:1
        (->> "UCASE('ab')"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :upper
            :args [
                {:type :character-string-literal, :value "'ab'"}
            ]
        }]
    )
    (:fact func:aggregate:count:1
        (->> "COUNT(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :count
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:sum:1
        (->> "SUM(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :sum
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:avg:1
        (->> "AVG(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :avg
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:min:1
        (->> "MIN(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :min
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:max:1
        (->> "MAX(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :max
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:variance:1
        (->> "VARIANCE(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :var_pop
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:var_pop:1
        (->> "VAR_POP(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :var_pop
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:var_samp:1
        (->> "VAR_SAMP(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :var_samp
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:stddev_pop:1
        (->> "STDDEV_POP(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :stddev_pop
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:stddev_samp:1
        (->> "STDDEV_SAMP(1)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :stddev_samp
            :args [
                {:type :numeric-literal, :value "1"}
            ]
        }]
    )
    (:fact func:aggregate:covar_pop:1
        (->> "COVAR_POP(1, 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :covar_pop
            :args [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
    (:fact func:aggregate:covar_samp:1
        (->> "COVAR_SAMP(1, 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :covar_samp
            :args [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
    (:fact func:aggregate:corr:1
        (->> "CORR(1, 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :corr
            :args [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
    (:fact func:aggregate:percentile:1
        (->> "PERCENTILE(1, 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :percentile
            :args [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
    (:fact func:aggregate:percentile_approx:1
        (->> "PERCENTILE_APPROX(1, 2)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :func-call, :func :percentile_approx
            :args [
                {:type :numeric-literal, :value "1"}
                {:type :numeric-literal, :value "2"}
            ]
        }]
    )
)
