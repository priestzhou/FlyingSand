(ns parser-unittest.parser.translator
    (:use 
        [testing.core :only (suite)]
    )
    (:require
        [clojure.string :as str]
        [utilities.parse :as prs]
        [parser.translator :as trans]
    )
    (:import
        [utilities.parse InvalidSyntaxException]
    )
)

(def context {
    :ns [{
        :type "namespace"
        :name "com"
        :children [{
            :type "namespace"
            :name "app"
            :children [{
                :type "namespace"
                :name "ver"
                :children [{
                    :type "table"
                    :name "tbl"
                    :hive-name "`com.app.ver.tbl`"
                    :children [
                        {
                            :name "col"
                            :type "varchar(255)"
                        }
                    ]
                }]
            }]
        }]
    }]
    :default-ns ["com" "app" "ver"]
})

(suite "quoted"
    (:fact quoted
        (trans/quoted \` "ab")
        :is
        "ab"
    )
    (:fact quoted:special-char
        (trans/quoted \` "a+b")
        :is
        "`a+b`"
    )
    (:fact quoted:escaped
        (trans/quoted \` "a`b")
        :is
        "`a``b`"
    )
)

(suite "normalize-table"
    (:fact normalize-table:only-table
        (->> ["tbl"]
            (trans/normalize-table context)
            (:hive-name)
        )
        :is
        "`com.app.ver.tbl`"
    )
    (:fact normalize-table:namespaced-table
        (->> ["ver" "tbl"]
            (trans/normalize-table context)
            (:hive-name)
        )
        :is
        "`com.app.ver.tbl`"
    )
    (:fact normalize-table:absolute
        (->> ["com" "app" "ver" "tbl"]
            (trans/normalize-table context)
            (:hive-name)
        )
        :is
        "`com.app.ver.tbl`"
    )
    (:fact normalize-table:unmatch
        (fn []
            (trans/normalize-table context ["lbt"])
        )
        :throws InvalidSyntaxException
    )
)

(suite "sql->hive"
    (:fact to-hive
        (trans/sql-2003->hive context "SELECT * FROM tbl")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:select:subquery
        (trans/sql-2003->hive context "SELECT (SELECT * FROM tbl) (col) FROM tbl")
        :is
        "SELECT (SELECT * FROM `com.app.ver.tbl` tbl) (col) FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:all
        (trans/sql-2003->hive context "SELECT ALL * FROM tbl")
        :is
        "SELECT ALL * FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:distinct
        (trans/sql-2003->hive context "SELECT DISTINCT * FROM tbl")
        :is
        "SELECT DISTINCT * FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:asterisk:table
        (trans/sql-2003->hive context "SELECT tbl.* FROM tbl")
        :is
        "SELECT tbl.* FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:asterisk:ns
        (trans/sql-2003->hive context "SELECT ver.tbl.* FROM ver.tbl")
        :is
        "SELECT `com.app.ver.tbl`.* FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:column:as
        (trans/sql-2003->hive context "SELECT col a FROM tbl")
        :is
        "SELECT col a FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:asterisk:as
        (trans/sql-2003->hive context "SELECT * a FROM tbl")
        :is
        "SELECT * a FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:asterisk:as:paren
        (trans/sql-2003->hive context "SELECT * (a) FROM tbl")
        :is
        "SELECT * (a) FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:table:as
        (trans/sql-2003->hive context "SELECT * FROM (SELECT * FROM tbl) AS t")
        :is
        "SELECT * FROM (SELECT * FROM `com.app.ver.tbl` tbl) t"
    )
    (:fact to-hive:select-column:raw-table
        (trans/sql-2003->hive context "SELECT col FROM tbl")
        :is
        "SELECT col FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:select-column:raw-table:selected
        (trans/sql-2003->hive context "SELECT tbl.col FROM tbl")
        :is
        "SELECT tbl.col FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:select-column:derived-table
        (trans/sql-2003->hive context "SELECT col FROM tbl AS tbl")
        :is
        "SELECT col FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:where
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE col<1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE col < 1"
    )
    (:fact to-hive:group-by
        (trans/sql-2003->hive context "SELECT * FROM tbl GROUP BY col, 1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl GROUP BY col, 1"
    )
    (:fact to-hive:order-by
        (trans/sql-2003->hive context "SELECT * FROM tbl ORDER BY col, col ASC, 1 DESC")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl ORDER BY col, col ASC, 1 DESC"
    )
    (:fact to-hive:limit
        (trans/sql-2003->hive context "SELECT * FROM tbl LIMIT 1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl LIMIT 1"
    )
    (:fact to-hive:union:all
        (trans/sql-2003->hive context "SELECT * FROM tbl UNION ALL SELECT * FROM tbl")
        :is
        "(SELECT * FROM `com.app.ver.tbl` tbl) UNION ALL (SELECT * FROM `com.app.ver.tbl` tbl)"
    )
    (:fact to-hive:cross-join
        (trans/sql-2003->hive context "SELECT * FROM tbl CROSS JOIN tbl")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl CROSS JOIN `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:cross-join:on
        (trans/sql-2003->hive context "SELECT * FROM tbl t1 CROSS JOIN tbl t2 ON t1.col=t2.col")
        :is
        "SELECT * FROM `com.app.ver.tbl` t1 CROSS JOIN `com.app.ver.tbl` t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:inner-join
        (trans/sql-2003->hive context "SELECT * FROM tbl JOIN tbl")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl JOIN `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:inner-join:on
        (trans/sql-2003->hive context "SELECT * FROM (tbl t1) JOIN (tbl t2) ON t1.col=t2.col")
        :is
        "SELECT * FROM `com.app.ver.tbl` t1 JOIN `com.app.ver.tbl` t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:join-after-join
        (trans/sql-2003->hive context 
            "SELECT * FROM (tbl t1) JOIN (tbl t2) ON t1.col=t2.col JOIN (tbl t3) ON t3.col=t2.col"
        )
        :is
        "SELECT * FROM `com.app.ver.tbl` t1 JOIN `com.app.ver.tbl` t2 ON (t1.col = t2.col) JOIN `com.app.ver.tbl` t3 ON (t3.col = t2.col)"
    )
    (:fact to-hive:outer-join:left
        (trans/sql-2003->hive context 
            "SELECT * FROM (tbl t1) LEFT JOIN (tbl t2) ON t1.col=t2.col"
        )
        :is
        "SELECT * FROM `com.app.ver.tbl` t1 LEFT JOIN `com.app.ver.tbl` t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:outer-join:right
        (trans/sql-2003->hive context 
            "SELECT * FROM (tbl t1) RIGHT JOIN (tbl t2) ON t1.col=t2.col"
        )
        :is
        "SELECT * FROM `com.app.ver.tbl` t1 RIGHT JOIN `com.app.ver.tbl` t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:outer-join:full
        (trans/sql-2003->hive context 
            "SELECT * FROM (tbl t1) FULL JOIN (tbl t2) ON t1.col=t2.col"
        )
        :is
        "SELECT * FROM `com.app.ver.tbl` t1 FULL JOIN `com.app.ver.tbl` t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:where:precedence
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE TRUE OR NOT FALSE AND UNKNOWN")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE TRUE OR (NOT FALSE) AND UNKNOWN"
    )
    (:fact to-hive:where:test
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE TRUE IS TRUE")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE TRUE IS TRUE"
    )
    (:fact to-hive:where:test:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE TRUE IS NOT FALSE")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE TRUE IS NOT FALSE"
    )
    (:fact to-hive:where:test:chain
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE FALSE IS NOT NULL IS TRUE")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE (FALSE IS NOT NULL) IS TRUE"
    )
    (:fact to-hive:where:test:null
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE TRUE IS NULL")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE TRUE IS NULL"
    )
    (:fact to-hive:where:=
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 = 1"
    )
    (:fact to-hive:where:comparison:<=>
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<=>1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 <=> 1"
    )
    (:fact to-hive:where:comparison:!=
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1!=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 <> 1"
    )
    (:fact to-hive:where:comparison:<>
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<>1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 <> 1"
    )
    (:fact to-hive:where:comparison:<
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 < 1"
    )
    (:fact to-hive:where:comparison:<=
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 <= 1"
    )
    (:fact to-hive:where:comparison:>
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1>1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 > 1"
    )
    (:fact to-hive:where:comparison:>=
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1>=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 >= 1"
    )
    (:fact to-hive:where:bit:|
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1|1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 | 1"
    )
    (:fact to-hive:where:bit:&
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1&1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 & 1"
    )
    (:fact to-hive:where:bit:<<
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<<1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 << 1"
    )
    (:fact to-hive:where:bit:>>
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1>>1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 >> 1"
    )
    (:fact to-hive:where:bit:caret
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1^1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 ^ 1"
    )
    (:fact to-hive:where:bit:unary-tilde
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE ~ 1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE ~ 1"
    )
    (:fact to-hive:where:arithmetic:+
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1+1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 + 1"
    )
    (:fact to-hive:where:arithmetic:-
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1-1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 - 1"
    )
    (:fact to-hive:where:arithmetic:*
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1*1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 * 1"
    )
    (:fact to-hive:where:arithmetic:div
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1/1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 / 1"
    )
    (:fact to-hive:where:arithmetic:%
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1%1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 % 1"
    )
    (:fact to-hive:where:arithmetic:unary+
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE + 1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE + 1"
    )
    (:fact to-hive:where:arithmetic:unary-
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE - 1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE - 1"
    )
    (:fact to-hive:where:predicate:in-array
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1 IN (1, 2)")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 1 IN (1, 2)"
    )
    (:fact to-hive:where:predicate:in-array:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 3 NOT IN (1, 2)")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 3 NOT IN (1, 2)"
    )
    (:fact to-hive:where:predicate:between
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 2 BETWEEN 1 AND 3")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 2 BETWEEN 1 AND 3"
    )
    (:fact to-hive:where:predicate:between:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 0 NOT BETWEEN 1 AND 3")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 0 NOT BETWEEN 1 AND 3"
    )
    (:fact to-hive:where:predicate:like
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 'a' LIKE 'a'")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 'a' LIKE 'a'"
    )
    (:fact to-hive:where:predicate:like:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 'a' NOT LIKE 'a'")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 'a' NOT LIKE 'a'"
    )
    (:fact to-hive:where:predicate:reglike
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 'a' REGEXP 'a'")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 'a' REGEXP 'a'"
    )
    (:fact to-hive:where:predicate:reglike:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 'a' NOT REGEXP 'b'")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE 'a' NOT REGEXP 'b'"
    )
    (:fact to-hive:where:simple-expr:binary
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE BINARY 'a' <> 'A'")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE BINARY('a') <> 'A'"
    )
    (:fact to-hive:where:simple-expr:exists
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE EXISTS (SELECT * FROM tbl)")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE EXISTS (SELECT * FROM `com.app.ver.tbl` tbl)"
    )
    (:fact to-hive:where:cast:int
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as int)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS INT) = 1"
    )
    (:fact to-hive:where:cast:tinyint
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as tinyint)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS TINYINT) = 1"
    )
    (:fact to-hive:where:cast:smallint
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as smallint)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS SMALLINT) = 1"
    )
    (:fact to-hive:where:cast:bigint
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as bigint)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS BIGINT) = 1"
    )
    (:fact to-hive:where:cast:float
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as float)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS FLOAT) = 1"
    )
    (:fact to-hive:where:cast:double
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as double)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS DOUBLE) = 1"
    )
    (:fact to-hive:where:cast:decimal
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as decimal)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS DECIMAL) = 1"
    )
    (:fact to-hive:where:cast:timestamp
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as timestamp)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS TIMESTAMP) = 1"
    )
    (:fact to-hive:where:cast:date
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as date)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS DATE) = 1"
    )
    (:fact to-hive:where:cast:string
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as string)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS STRING) = 1"
    )
    (:fact to-hive:where:cast:varchar
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as varchar)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS VARCHAR) = 1"
    )
    (:fact to-hive:where:cast:boolean
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as boolean)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS BOOLEAN) = 1"
    )
    (:fact to-hive:where:cast:binary
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as binary)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CAST('1' AS BINARY) = 1"
    )
    (:fact to-hive:where:case
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE case when TRUE then FALSE end")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CASE WHEN TRUE THEN FALSE END"
    )
    (:fact to-hive:where:case:else
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE case when 1 then 2 else 3 end")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CASE WHEN 1 THEN 2 ELSE 3 END"
    )
    (:fact to-hive:where:case:value
        (trans/sql-2003->hive context "SELECT col FROM tbl WHERE case col when 1 then TRUE when 0 then FALSE end")
        :is
        "SELECT col FROM `com.app.ver.tbl` tbl WHERE CASE col WHEN 1 THEN TRUE WHEN 0 THEN FALSE END"
    )
    (:fact to-hive:select:distinct-count:1
        (trans/sql-2003->hive context "SELECT count(distinct 0) FROM tbl")
        :is
        "SELECT COUNT(DISTINCT 0) FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:select:distinct-count:2
        (trans/sql-2003->hive context "SELECT count(distinct 0, 1) FROM tbl")
        :is
        "SELECT COUNT(DISTINCT 0, 1) FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:select:distinct-sum:1
        (trans/sql-2003->hive context "SELECT sum(distinct col) FROM tbl")
        :is
        "SELECT SUM(DISTINCT col) FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:select:distinct-avg:1
        (trans/sql-2003->hive context "SELECT avg(distinct col) FROM tbl")
        :is
        "SELECT AVG(DISTINCT col) FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:where:pow
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE pow(1, 0)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE POWER(1, 0) = 1"
    )
    (:fact to-hive:where:power
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE power(1, 0)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE POWER(1, 0) = 1"
    )
    (:fact to-hive:where:round:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE round(1)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE ROUND(1) = 1"
    )
    (:fact to-hive:where:round:2
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE round(1, 0)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE ROUND(1, 0) = 1"
    )
    (:fact to-hive:where:floor:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE floor(1)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE FLOOR(1) = 1"
    )
    (:fact to-hive:where:ceil:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE ceil(1)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CEIL(1) = 1"
    )
    (:fact to-hive:where:ceiling:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE ceiling(1)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CEIL(1) = 1"
    )
    (:fact to-hive:where:rand:0
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE rand()=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE RAND() = 1"
    )
    (:fact to-hive:where:rand:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE rand(0)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE RAND(0) = 1"
    )
    (:fact to-hive:where:exp:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE exp(0)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE EXP(0) = 1"
    )
    (:fact to-hive:where:ln:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE ln(1)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE LN(1) = 0"
    )
    (:fact to-hive:where:log10:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE log10(1)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE LOG10(1) = 0"
    )
    (:fact to-hive:where:log2:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE log2(1)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE LOG2(1) = 0"
    )
    (:fact to-hive:where:log:2
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE log(1, 2)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE LOG(1, 2) = 0"
    )
    (:fact to-hive:where:sqrt:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE sqrt(1)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE SQRT(1) = 1"
    )
    (:fact to-hive:where:bin:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE bin(1)='1'")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE BIN(1) = '1'"
    )
    (:fact to-hive:where:hex:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE hex(1)='1'")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE HEX(1) = '1'"
    )
    (:fact to-hive:where:unhex:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE unhex('1')=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE UNHEX('1') = 1"
    )
    (:fact to-hive:where:conv:3
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE conv(1,2,3)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE CONV(1, 2, 3) = 1"
    )
    (:fact to-hive:where:abs:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE abs(1)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE ABS(1) = 1"
    )
    (:fact to-hive:where:pmod:2
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE pmod(1,2)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE PMOD(1, 2) = 1"
    )
    (:fact to-hive:where:sin:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE sin(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE SIN(0) = 0"
    )
    (:fact to-hive:where:asin:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE asin(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE ASIN(0) = 0"
    )
    (:fact to-hive:where:cos:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cos(0)=1")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE COS(0) = 1"
    )
    (:fact to-hive:where:acos:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE acos(1)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE ACOS(1) = 0"
    )
    (:fact to-hive:where:tan:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE tan(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE TAN(0) = 0"
    )
    (:fact to-hive:where:atan:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE atan(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE ATAN(0) = 0"
    )
    (:fact to-hive:where:degrees:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE degrees(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE DEGREES(0) = 0"
    )
    (:fact to-hive:where:radians:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE radians(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE RADIANS(0) = 0"
    )
    (:fact to-hive:where:positive:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE positive(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE POSITIVE(0) = 0"
    )
    (:fact to-hive:where:negative:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE negative(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE NEGATIVE(0) = 0"
    )
    (:fact to-hive:where:sign:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE sign(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE SIGN(0) = 0"
    )
    (:fact to-hive:where:e:0
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE e()<>0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE E() <> 0"
    )
    (:fact to-hive:where:pi:0
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE pi()<>0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE PI() <> 0"
    )
    (:fact to-hive:where:binary:1
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE binary(0)=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE BINARY(0) = 0"
    )
    (:fact to-hive:select:from_unixtime:1
        (trans/sql-2003->hive context "SELECT from_unixtime(col) FROM tbl")
        :is
        "SELECT FROM_UNIXTIME(col) FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:select:from_unixtime:2
        (trans/sql-2003->hive context "SELECT from_unixtime(col, 'YYYY') FROM tbl")
        :is
        "SELECT FROM_UNIXTIME(col, 'YYYY') FROM `com.app.ver.tbl` tbl"
    )
    (:fact to-hive:where:unix_timestamp:0
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE unix_timestamp()=0")
        :is
        "SELECT * FROM `com.app.ver.tbl` tbl WHERE UNIX_TIMESTAMP() = 0"
    )
)
