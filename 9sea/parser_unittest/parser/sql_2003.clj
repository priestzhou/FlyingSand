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
        [[:eof] {:type :select, :select-list :asterisk,
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
        [[:eof] {:type :select, :set-quantifier :distinct, :select-list :asterisk,
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
        [[:eof] {:type :select, :set-quantifier :all, :select-list :asterisk,
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
            :select-list [{:type :qualified-asterisk, :refer ["ns" "tbl"]}]
        }]
    )
    (:fact select:all-fields-reference
        (->> "SELECT 1.* FROM ns.tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :from-clause [{:type :table, :refer ["ns" "tbl"]}]
            :select-list [
                {:type :all-fields-reference, :value {:type :numeric-literal, :value "1"}}
            ]
        }]
    )
    (:fact select:all-fields-reference:as
        (->> "SELECT 1.* AS (a) FROM ns.tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :from-clause [{:type :table, :refer ["ns" "tbl"]}]
            :select-list [
                {
                    :type :all-fields-reference, :names ["a"],
                    :value {:type :numeric-literal, :value "1"}
                }
            ]
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
            :select-list [
                {
                    :type :derived-column, :name "a",
                    :value {:type :numeric-literal, :value "1"}
                }
            ]
        }]
    )
    (:fact select:derived-column:as:no-as
        (->> "SELECT 1 AS a FROM ns.tbl"
            (prs/str->stream)
            (sql/query)
            (extract-result)
        )
        :is
        [[:eof] {:type :select
            :from-clause [{:type :table, :refer ["ns" "tbl"]}]
            :select-list [
                {
                    :type :derived-column, :name "a",
                    :value {:type :numeric-literal, :value "1"}
                }
            ]
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
            :select-list :asterisk
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
            :select-list :asterisk
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
            :select-list :asterisk
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
            :select-list :asterisk
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
                :select-list :asterisk
                :from-clause [{:type :table, :refer ["tbl"]}]
            } {
                :type :select
                :select-list :asterisk
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
                :select-list :asterisk
                :from-clause [{:type :table, :refer ["tbl"]}]
            } {
                :type :select
                :select-list :asterisk
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
    (:fact expr:subquery
        (->> "(SELECT * FROM tbl)"
            (prs/str->stream)
            (sql/value-expr)
            (extract-result)
        )
        :is
        [[:eof] {:type :select, :select-list :asterisk,
            :from-clause [{:type :table, :refer ["tbl"]}]
        }]
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
)
