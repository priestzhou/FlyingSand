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
        :name "app"
        :children [{
            :type "namespace"
            :name "ver"
            :children [{
                :type "table"
                :name "tbl"
                :hive-name "hivetbl"
                :children [
                    {
                        :name "col"
                        :type "varchar(255)"
                    }
                ]
            } {
                :type "table"
                :name "tbl1"
                :hive-name "hivetbl1"
                :children [
                    {
                        :name "col"
                        :type "varchar(255)"
                    }
                ]
            }]
        }]
    }]
    :default-ns ["app" "ver"]
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
        "hivetbl"
    )
    (:fact normalize-table:namespaced-table
        (->> ["ver" "tbl"]
            (trans/normalize-table context)
            (:hive-name)
        )
        :is
        "hivetbl"
    )
    (:fact normalize-table:absolute
        (->> ["app" "ver" "tbl"]
            (trans/normalize-table context)
            (:hive-name)
        )
        :is
        "hivetbl"
    )
    (:fact normalize-table:ns
        (fn []
            (trans/normalize-table context ["ver"])
        )
        :throws InvalidSyntaxException
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
        "SELECT * FROM hivetbl"
    )
    (:fact to-hive:select:subquery
        (trans/sql-2003->hive context "SELECT (SELECT * FROM tbl) (col) FROM tbl")
        :is
        "SELECT (SELECT * FROM hivetbl) (col) FROM hivetbl"
    )
    (:fact to-hive:all
        (trans/sql-2003->hive context "SELECT ALL * FROM tbl")
        :is
        "SELECT ALL * FROM hivetbl"
    )
    (:fact to-hive:distinct
        (trans/sql-2003->hive context "SELECT DISTINCT * FROM tbl")
        :is
        "SELECT DISTINCT * FROM hivetbl"
    )
    (:fact to-hive:asterisk:table
        (trans/sql-2003->hive context "SELECT tbl.* FROM tbl")
        :is
        "SELECT hivetbl.* FROM hivetbl"
    )
    (:fact to-hive:asterisk:ns
        (trans/sql-2003->hive context "SELECT ver.tbl.* FROM ver.tbl")
        :is
        "SELECT hivetbl.* FROM hivetbl"
    )
    (:fact to-hive:column:as
        (trans/sql-2003->hive context "SELECT col a FROM tbl")
        :is
        "SELECT col a FROM hivetbl"
    )
    (:fact to-hive:asterisk:as
        (trans/sql-2003->hive context "SELECT * a FROM tbl")
        :is
        "SELECT * a FROM hivetbl"
    )
    (:fact to-hive:asterisk:as:paren
        (trans/sql-2003->hive context "SELECT * (a) FROM tbl")
        :is
        "SELECT * (a) FROM hivetbl"
    )
    (:fact to-hive:table:as
        (trans/sql-2003->hive context "SELECT * FROM (SELECT * FROM tbl) AS t")
        :is
        "SELECT * FROM (SELECT * FROM hivetbl) t"
    )
    (:fact to-hive:select-column:raw-table
        (trans/sql-2003->hive context "SELECT col FROM tbl")
        :is
        "SELECT col FROM hivetbl"
    )
    (:fact to-hive:select-column:raw-table:selected
        (trans/sql-2003->hive context "SELECT tbl.col FROM tbl")
        :is
        "SELECT hivetbl.col FROM hivetbl"
    )
    (:fact to-hive:select-column:derived-table
        (trans/sql-2003->hive context "SELECT col FROM tbl AS tbl")
        :is
        "SELECT col FROM hivetbl tbl"
    )
    (:fact to-hive:where
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE col<1")
        :is
        "SELECT * FROM hivetbl WHERE col < 1"
    )
    (:fact to-hive:group-by
        (trans/sql-2003->hive context "SELECT * FROM tbl GROUP BY col, 1")
        :is
        "SELECT * FROM hivetbl GROUP BY col, 1"
    )
    (:fact to-hive:order-by
        (trans/sql-2003->hive context "SELECT * FROM tbl ORDER BY col, col ASC, 1 DESC")
        :is
        "SELECT * FROM hivetbl ORDER BY col, col ASC, 1 DESC"
    )
    (:fact to-hive:limit
        (trans/sql-2003->hive context "SELECT * FROM tbl LIMIT 1")
        :is
        "SELECT * FROM hivetbl LIMIT 1"
    )
    (:fact to-hive:union:all
        (trans/sql-2003->hive context "SELECT * FROM tbl UNION ALL SELECT * FROM tbl")
        :is
        "(SELECT * FROM hivetbl) UNION ALL (SELECT * FROM hivetbl)"
    )
    (:fact to-hive:cross-join
        (trans/sql-2003->hive context "SELECT * FROM tbl AS t1 CROSS JOIN tbl AS t2")
        :is
        "SELECT * FROM (hivetbl t1) CROSS JOIN (hivetbl t2)"
    )
    (:fact to-hive:cross-join:on
        (trans/sql-2003->hive context "SELECT * FROM tbl t1 CROSS JOIN tbl t2 ON t1.col=t2.col")
        :is
        "SELECT * FROM (hivetbl t1) CROSS JOIN (hivetbl t2) ON (t1.col = t2.col)"
    )
    (:fact to-hive:inner-join
        (trans/sql-2003->hive context "SELECT * FROM tbl t1 JOIN tbl t2")
        :is
        "SELECT * FROM (hivetbl t1) JOIN (hivetbl t2)"
    )
    (:fact to-hive:inner-join:on
        (trans/sql-2003->hive context "SELECT * FROM tbl JOIN tbl1 ON tbl.col=tbl1.col")
        :is
        "SELECT * FROM hivetbl JOIN hivetbl1 ON (hivetbl.col = hivetbl1.col)"
    )
    (:fact to-hive:inner-join:on:rename
        (trans/sql-2003->hive context "SELECT * FROM (tbl t1) JOIN (tbl t2) ON t1.col=t2.col")
        :is
        "SELECT * FROM (hivetbl t1) JOIN (hivetbl t2) ON (t1.col = t2.col)"
    )
    (:fact to-hive:join-after-join
        (trans/sql-2003->hive context 
            "SELECT * FROM (tbl t1) JOIN (tbl t2) ON t1.col=t2.col JOIN (tbl t3) ON t3.col=t2.col"
        )
        :is
        "SELECT * FROM (hivetbl t1) JOIN (hivetbl t2) ON (t1.col = t2.col) JOIN (hivetbl t3) ON (t3.col = t2.col)"
    )
    (:fact to-hive:outer-join:left
        (trans/sql-2003->hive context 
            "SELECT * FROM (tbl t1) LEFT JOIN (tbl t2) ON t1.col=t2.col"
        )
        :is
        "SELECT * FROM (hivetbl t1) LEFT JOIN (hivetbl t2) ON (t1.col = t2.col)"
    )
    (:fact to-hive:outer-join:right
        (trans/sql-2003->hive context 
            "SELECT * FROM (tbl t1) RIGHT JOIN (tbl t2) ON t1.col=t2.col"
        )
        :is
        "SELECT * FROM (hivetbl t1) RIGHT JOIN (hivetbl t2) ON (t1.col = t2.col)"
    )
    (:fact to-hive:outer-join:full
        (trans/sql-2003->hive context 
            "SELECT * FROM (tbl t1) FULL JOIN (tbl t2) ON t1.col=t2.col"
        )
        :is
        "SELECT * FROM (hivetbl t1) FULL JOIN (hivetbl t2) ON (t1.col = t2.col)"
    )
    (:fact to-hive:where:precedence
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE TRUE OR NOT FALSE AND UNKNOWN")
        :is
        "SELECT * FROM hivetbl WHERE TRUE OR (NOT FALSE) AND UNKNOWN"
    )
    (:fact to-hive:where:test
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE TRUE IS TRUE")
        :is
        "SELECT * FROM hivetbl WHERE TRUE IS TRUE"
    )
    (:fact to-hive:where:test:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE TRUE IS NOT FALSE")
        :is
        "SELECT * FROM hivetbl WHERE TRUE IS NOT FALSE"
    )
    (:fact to-hive:where:test:chain
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE FALSE IS NOT NULL IS TRUE")
        :is
        "SELECT * FROM hivetbl WHERE (FALSE IS NOT NULL) IS TRUE"
    )
    (:fact to-hive:where:test:null
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE TRUE IS NULL")
        :is
        "SELECT * FROM hivetbl WHERE TRUE IS NULL"
    )
    (:fact to-hive:where:=
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1=1")
        :is
        "SELECT * FROM hivetbl WHERE 1 = 1"
    )
    (:fact to-hive:where:comparison:<=>
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<=>1")
        :is
        "SELECT * FROM hivetbl WHERE 1 <=> 1"
    )
    (:fact to-hive:where:comparison:!=
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1!=1")
        :is
        "SELECT * FROM hivetbl WHERE 1 <> 1"
    )
    (:fact to-hive:where:comparison:<>
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<>1")
        :is
        "SELECT * FROM hivetbl WHERE 1 <> 1"
    )
    (:fact to-hive:where:comparison:<
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<1")
        :is
        "SELECT * FROM hivetbl WHERE 1 < 1"
    )
    (:fact to-hive:where:comparison:<=
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<=1")
        :is
        "SELECT * FROM hivetbl WHERE 1 <= 1"
    )
    (:fact to-hive:where:comparison:>
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1>1")
        :is
        "SELECT * FROM hivetbl WHERE 1 > 1"
    )
    (:fact to-hive:where:comparison:>=
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1>=1")
        :is
        "SELECT * FROM hivetbl WHERE 1 >= 1"
    )
    (:fact to-hive:where:bit:|
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1|1")
        :is
        "SELECT * FROM hivetbl WHERE 1 | 1"
    )
    (:fact to-hive:where:bit:&
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1&1")
        :is
        "SELECT * FROM hivetbl WHERE 1 & 1"
    )
    (:fact to-hive:where:bit:<<
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1<<1")
        :is
        "SELECT * FROM hivetbl WHERE 1 << 1"
    )
    (:fact to-hive:where:bit:>>
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1>>1")
        :is
        "SELECT * FROM hivetbl WHERE 1 >> 1"
    )
    (:fact to-hive:where:bit:caret
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1^1")
        :is
        "SELECT * FROM hivetbl WHERE 1 ^ 1"
    )
    (:fact to-hive:where:bit:unary-tilde
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE ~ 1")
        :is
        "SELECT * FROM hivetbl WHERE ~ 1"
    )
    (:fact to-hive:where:arithmetic:+
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1+1")
        :is
        "SELECT * FROM hivetbl WHERE 1 + 1"
    )
    (:fact to-hive:where:arithmetic:-
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1-1")
        :is
        "SELECT * FROM hivetbl WHERE 1 - 1"
    )
    (:fact to-hive:where:arithmetic:*
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1*1")
        :is
        "SELECT * FROM hivetbl WHERE 1 * 1"
    )
    (:fact to-hive:where:arithmetic:div
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1/1")
        :is
        "SELECT * FROM hivetbl WHERE 1 / 1"
    )
    (:fact to-hive:where:arithmetic:%
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1%1")
        :is
        "SELECT * FROM hivetbl WHERE 1 % 1"
    )
    (:fact to-hive:where:arithmetic:unary+
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE + 1")
        :is
        "SELECT * FROM hivetbl WHERE + 1"
    )
    (:fact to-hive:where:arithmetic:unary-
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE - 1")
        :is
        "SELECT * FROM hivetbl WHERE - 1"
    )
    (:fact to-hive:where:predicate:in-array
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 1 IN (1, 2)")
        :is
        "SELECT * FROM hivetbl WHERE 1 IN (1, 2)"
    )
    (:fact to-hive:where:predicate:in-array:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 3 NOT IN (1, 2)")
        :is
        "SELECT * FROM hivetbl WHERE 3 NOT IN (1, 2)"
    )
    (:fact to-hive:where:predicate:between
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 2 BETWEEN 1 AND 3")
        :is
        "SELECT * FROM hivetbl WHERE 2 BETWEEN 1 AND 3"
    )
    (:fact to-hive:where:predicate:between:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 0 NOT BETWEEN 1 AND 3")
        :is
        "SELECT * FROM hivetbl WHERE 0 NOT BETWEEN 1 AND 3"
    )
    (:fact to-hive:where:predicate:like
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 'a' LIKE 'a'")
        :is
        "SELECT * FROM hivetbl WHERE 'a' LIKE 'a'"
    )
    (:fact to-hive:where:predicate:like:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 'a' NOT LIKE 'a'")
        :is
        "SELECT * FROM hivetbl WHERE 'a' NOT LIKE 'a'"
    )
    (:fact to-hive:where:predicate:reglike
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 'a' REGEXP 'a'")
        :is
        "SELECT * FROM hivetbl WHERE 'a' REGEXP 'a'"
    )
    (:fact to-hive:where:predicate:reglike:not
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE 'a' NOT REGEXP 'b'")
        :is
        "SELECT * FROM hivetbl WHERE 'a' NOT REGEXP 'b'"
    )
    (:fact to-hive:where:simple-expr:binary
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE BINARY 'a' <> 'A'")
        :is
        "SELECT * FROM hivetbl WHERE BINARY('a') <> 'A'"
    )
    (:fact to-hive:where:simple-expr:exists
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE EXISTS (SELECT * FROM tbl)")
        :is
        "SELECT * FROM hivetbl WHERE EXISTS (SELECT * FROM hivetbl)"
    )
    (:fact to-hive:where:cast:int
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE cast('1' as int)=1")
        :is
        "SELECT * FROM hivetbl WHERE CAST('1' AS INT) = 1"
    )
    (:fact to-hive:where:case
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE case when TRUE then FALSE end")
        :is
        "SELECT * FROM hivetbl WHERE CASE WHEN TRUE THEN FALSE END"
    )
    (:fact to-hive:where:case:else
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE case when 1 then 2 else 3 end")
        :is
        "SELECT * FROM hivetbl WHERE CASE WHEN 1 THEN 2 ELSE 3 END"
    )
    (:fact to-hive:where:case:value
        (trans/sql-2003->hive context "SELECT col FROM tbl WHERE case col when 1 then TRUE when 0 then FALSE end")
        :is
        "SELECT col FROM hivetbl WHERE CASE col WHEN 1 THEN TRUE WHEN 0 THEN FALSE END"
    )
    (:fact to-hive:select:distinct-count:1
        (trans/sql-2003->hive context "SELECT count(distinct col) FROM tbl")
        :is
        "SELECT COUNT(DISTINCT col) FROM hivetbl"
    )
    (:fact to-hive:select:distinct-count:2
        (trans/sql-2003->hive context "SELECT count(distinct col, 1) FROM tbl")
        :is
        "SELECT COUNT(DISTINCT col, 1) FROM hivetbl"
    )
    (:fact to-hive:select:distinct-sum:1
        (trans/sql-2003->hive context "SELECT sum(distinct col) FROM tbl")
        :is
        "SELECT SUM(DISTINCT col) FROM hivetbl"
    )
    (:fact to-hive:select:distinct-avg:1
        (trans/sql-2003->hive context "SELECT avg(distinct col) FROM tbl")
        :is
        "SELECT AVG(DISTINCT col) FROM hivetbl"
    )
    (:fact to-hive:where:pow
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE pow(1, 0)=1")
        :is
        "SELECT * FROM hivetbl WHERE POWER(1, 0) = 1"
    )
    (:fact to-hive:where:power
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE power(1, 0)=1")
        :is
        "SELECT * FROM hivetbl WHERE POWER(1, 0) = 1"
    )
    (:fact to-hive:where:rand:0
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE rand()=1")
        :is
        "SELECT * FROM hivetbl WHERE RAND() = 1"
    )
    (:fact to-hive:where:e:0
        (trans/sql-2003->hive context "SELECT * FROM tbl WHERE e()<>0")
        :is
        "SELECT * FROM hivetbl WHERE E() <> 0"
    )
    (:fact to-hive:select:from_unixtime:1
        (trans/sql-2003->hive context "SELECT from_unixtime(col) FROM tbl")
        :is
        "SELECT FROM_UNIXTIME(col) FROM hivetbl"
    )
)
