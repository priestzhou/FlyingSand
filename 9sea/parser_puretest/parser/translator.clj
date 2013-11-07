(ns parser-puretest.parser.translator
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

(defn- sql->hive [context sql-text]
    (->> sql-text
        (trans/parse-sql context)
        (trans/dump-hive context)
    )
)

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
        (sql->hive context "SELECT * FROM tbl")
        :is
        "SELECT * FROM hivetbl"
    )
    (:fact to-hive:select:subquery
        (sql->hive context "SELECT (SELECT * FROM tbl) (col) FROM tbl")
        :is
        "SELECT (SELECT * FROM hivetbl) (col) FROM hivetbl"
    )
    (:fact to-hive:all
        (sql->hive context "SELECT ALL * FROM tbl")
        :is
        "SELECT ALL * FROM hivetbl"
    )
    (:fact to-hive:distinct
        (sql->hive context "SELECT DISTINCT * FROM tbl")
        :is
        "SELECT DISTINCT * FROM hivetbl"
    )
    (:fact to-hive:asterisk:table
        (sql->hive context "SELECT tbl.* FROM tbl")
        :is
        "SELECT hivetbl.* FROM hivetbl"
    )
    (:fact to-hive:asterisk:ns
        (sql->hive context "SELECT ver.tbl.* FROM ver.tbl")
        :is
        "SELECT hivetbl.* FROM hivetbl"
    )
    (:fact to-hive:column:as
        (sql->hive context "SELECT col a FROM tbl")
        :is
        "SELECT col a FROM hivetbl"
    )
    (:fact to-hive:asterisk:as
        (sql->hive context "SELECT * a FROM tbl")
        :is
        "SELECT * a FROM hivetbl"
    )
    (:fact to-hive:asterisk:as:paren
        (sql->hive context "SELECT * (a) FROM tbl")
        :is
        "SELECT * (a) FROM hivetbl"
    )
    (:fact to-hive:table:as
        (sql->hive context "SELECT * FROM (SELECT * FROM tbl) AS t")
        :is
        "SELECT * FROM (SELECT * FROM hivetbl) t"
    )
    (:fact to-hive:select-column:raw-table
        (sql->hive context "SELECT col FROM tbl")
        :is
        "SELECT col FROM hivetbl"
    )
    (:fact to-hive:select-column:raw-table:selected
        (sql->hive context "SELECT tbl.col FROM tbl")
        :is
        "SELECT hivetbl.col FROM hivetbl"
    )
    (:fact to-hive:select-column:derived-table
        (sql->hive context "SELECT col FROM tbl AS tbl")
        :is
        "SELECT col FROM hivetbl tbl"
    )
    (:fact to-hive:where
        (sql->hive context "SELECT * FROM tbl WHERE col<1")
        :is
        "SELECT * FROM hivetbl WHERE col < 1"
    )
    (:fact to-hive:group-by
        (sql->hive context "SELECT * FROM tbl GROUP BY col, 1")
        :is
        "SELECT * FROM hivetbl GROUP BY col, 1"
    )
    (:fact to-hive:order-by
        (sql->hive context "SELECT * FROM tbl ORDER BY col, col ASC, 1 DESC")
        :is
        "SELECT * FROM hivetbl ORDER BY col, col ASC, 1 DESC"
    )
    (:fact to-hive:limit
        (sql->hive context "SELECT * FROM tbl LIMIT 1")
        :is
        "SELECT * FROM hivetbl LIMIT 1"
    )
    (:fact to-hive:union:all
        (sql->hive context "SELECT * FROM tbl UNION ALL SELECT * FROM tbl")
        :is
        "(SELECT * FROM hivetbl) UNION ALL (SELECT * FROM hivetbl)"
    )
    (:fact to-hive:cross-join
        (sql->hive context "SELECT * FROM tbl AS t1 CROSS JOIN tbl AS t2")
        :is
        "SELECT * FROM hivetbl t1 CROSS JOIN hivetbl t2"
    )
    (:fact to-hive:cross-join:on
        (sql->hive context "SELECT * FROM tbl t1 CROSS JOIN tbl t2 ON t1.col=t2.col")
        :is
        "SELECT * FROM hivetbl t1 CROSS JOIN hivetbl t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:inner-join
        (sql->hive context "SELECT * FROM tbl t1 JOIN tbl t2")
        :is
        "SELECT * FROM hivetbl t1 JOIN hivetbl t2"
    )
    (:fact to-hive:inner-join:on
        (sql->hive context "SELECT * FROM tbl JOIN tbl1 ON tbl.col=tbl1.col")
        :is
        "SELECT * FROM hivetbl JOIN hivetbl1 ON (hivetbl.col = hivetbl1.col)"
    )
    (:fact to-hive:inner-join:on:rename
        (sql->hive context "SELECT * FROM (tbl t1) JOIN (tbl t2) ON t1.col=t2.col")
        :is
        "SELECT * FROM hivetbl t1 JOIN hivetbl t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:join-after-join
        (sql->hive context
            "SELECT * FROM (tbl t1) JOIN (tbl t2) ON t1.col=t2.col JOIN (tbl t3) ON t3.col=t2.col"
        )
        :is
        "SELECT * FROM hivetbl t1 JOIN hivetbl t2 ON (t1.col = t2.col) JOIN hivetbl t3 ON (t3.col = t2.col)"
    )
    (:fact to-hive:outer-join:left
        (sql->hive context
            "SELECT * FROM (tbl t1) LEFT JOIN (tbl t2) ON t1.col=t2.col"
        )
        :is
        "SELECT * FROM hivetbl t1 LEFT JOIN hivetbl t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:outer-join:right
        (sql->hive context
            "SELECT * FROM (tbl t1) RIGHT JOIN (tbl t2) ON t1.col=t2.col"
        )
        :is
        "SELECT * FROM hivetbl t1 RIGHT JOIN hivetbl t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:outer-join:full
        (sql->hive context
            "SELECT * FROM (tbl t1) FULL JOIN (tbl t2) ON t1.col=t2.col"
        )
        :is
        "SELECT * FROM hivetbl t1 FULL JOIN hivetbl t2 ON (t1.col = t2.col)"
    )
    (:fact to-hive:where:precedence
        (sql->hive context "SELECT * FROM tbl WHERE TRUE OR NOT FALSE AND UNKNOWN")
        :is
        "SELECT * FROM hivetbl WHERE TRUE OR (NOT FALSE) AND UNKNOWN"
    )
    (:fact to-hive:where:test
        (sql->hive context "SELECT * FROM tbl WHERE TRUE IS TRUE")
        :is
        "SELECT * FROM hivetbl WHERE TRUE IS TRUE"
    )
    (:fact to-hive:where:test:not
        (sql->hive context "SELECT * FROM tbl WHERE TRUE IS NOT FALSE")
        :is
        "SELECT * FROM hivetbl WHERE TRUE IS NOT FALSE"
    )
    (:fact to-hive:where:test:chain
        (sql->hive context "SELECT * FROM tbl WHERE FALSE IS NOT NULL IS TRUE")
        :is
        "SELECT * FROM hivetbl WHERE (FALSE IS NOT NULL) IS TRUE"
    )
    (:fact to-hive:where:test:null
        (sql->hive context "SELECT * FROM tbl WHERE TRUE IS NULL")
        :is
        "SELECT * FROM hivetbl WHERE TRUE IS NULL"
    )
    (:fact to-hive:where:=
        (sql->hive context "SELECT * FROM tbl WHERE 1=1")
        :is
        "SELECT * FROM hivetbl WHERE 1 = 1"
    )
    (:fact to-hive:where:comparison:<=>
        (sql->hive context "SELECT * FROM tbl WHERE 1<=>1")
        :is
        "SELECT * FROM hivetbl WHERE 1 <=> 1"
    )
    (:fact to-hive:where:comparison:!=
        (sql->hive context "SELECT * FROM tbl WHERE 1!=1")
        :is
        "SELECT * FROM hivetbl WHERE 1 <> 1"
    )
    (:fact to-hive:where:comparison:<>
        (sql->hive context "SELECT * FROM tbl WHERE 1<>1")
        :is
        "SELECT * FROM hivetbl WHERE 1 <> 1"
    )
    (:fact to-hive:where:comparison:<
        (sql->hive context "SELECT * FROM tbl WHERE 1<1")
        :is
        "SELECT * FROM hivetbl WHERE 1 < 1"
    )
    (:fact to-hive:where:comparison:<=
        (sql->hive context "SELECT * FROM tbl WHERE 1<=1")
        :is
        "SELECT * FROM hivetbl WHERE 1 <= 1"
    )
    (:fact to-hive:where:comparison:>
        (sql->hive context "SELECT * FROM tbl WHERE 1>1")
        :is
        "SELECT * FROM hivetbl WHERE 1 > 1"
    )
    (:fact to-hive:where:comparison:>=
        (sql->hive context "SELECT * FROM tbl WHERE 1>=1")
        :is
        "SELECT * FROM hivetbl WHERE 1 >= 1"
    )
    (:fact to-hive:where:bit:|
        (sql->hive context "SELECT * FROM tbl WHERE 1|1")
        :is
        "SELECT * FROM hivetbl WHERE 1 | 1"
    )
    (:fact to-hive:where:bit:&
        (sql->hive context "SELECT * FROM tbl WHERE 1&1")
        :is
        "SELECT * FROM hivetbl WHERE 1 & 1"
    )
    (:fact to-hive:where:bit:<<
        (sql->hive context "SELECT * FROM tbl WHERE 1<<1")
        :is
        "SELECT * FROM hivetbl WHERE 1 << 1"
    )
    (:fact to-hive:where:bit:>>
        (sql->hive context "SELECT * FROM tbl WHERE 1>>1")
        :is
        "SELECT * FROM hivetbl WHERE 1 >> 1"
    )
    (:fact to-hive:where:bit:caret
        (sql->hive context "SELECT * FROM tbl WHERE 1^1")
        :is
        "SELECT * FROM hivetbl WHERE 1 ^ 1"
    )
    (:fact to-hive:where:bit:unary-tilde
        (sql->hive context "SELECT * FROM tbl WHERE ~ 1")
        :is
        "SELECT * FROM hivetbl WHERE ~ 1"
    )
    (:fact to-hive:where:arithmetic:+
        (sql->hive context "SELECT * FROM tbl WHERE 1+1")
        :is
        "SELECT * FROM hivetbl WHERE 1 + 1"
    )
    (:fact to-hive:where:arithmetic:-
        (sql->hive context "SELECT * FROM tbl WHERE 1-1")
        :is
        "SELECT * FROM hivetbl WHERE 1 - 1"
    )
    (:fact to-hive:where:arithmetic:*
        (sql->hive context "SELECT * FROM tbl WHERE 1*1")
        :is
        "SELECT * FROM hivetbl WHERE 1 * 1"
    )
    (:fact to-hive:where:arithmetic:div
        (sql->hive context "SELECT * FROM tbl WHERE 1/1")
        :is
        "SELECT * FROM hivetbl WHERE 1 / 1"
    )
    (:fact to-hive:where:arithmetic:%
        (sql->hive context "SELECT * FROM tbl WHERE 1%1")
        :is
        "SELECT * FROM hivetbl WHERE 1 % 1"
    )
    (:fact to-hive:where:arithmetic:unary+
        (sql->hive context "SELECT * FROM tbl WHERE + 1")
        :is
        "SELECT * FROM hivetbl WHERE + 1"
    )
    (:fact to-hive:where:arithmetic:unary-
        (sql->hive context "SELECT * FROM tbl WHERE - 1")
        :is
        "SELECT * FROM hivetbl WHERE - 1"
    )
    (:fact to-hive:where:predicate:in-array
        (sql->hive context "SELECT * FROM tbl WHERE 1 IN (1, 2)")
        :is
        "SELECT * FROM hivetbl WHERE 1 IN (1, 2)"
    )
    (:fact to-hive:where:predicate:in-array:not
        (sql->hive context "SELECT * FROM tbl WHERE 3 NOT IN (1, 2)")
        :is
        "SELECT * FROM hivetbl WHERE 3 NOT IN (1, 2)"
    )
    (:fact to-hive:where:predicate:between
        (sql->hive context "SELECT * FROM tbl WHERE 2 BETWEEN 1 AND 3")
        :is
        "SELECT * FROM hivetbl WHERE 2 BETWEEN 1 AND 3"
    )
    (:fact to-hive:where:predicate:between:not
        (sql->hive context "SELECT * FROM tbl WHERE 0 NOT BETWEEN 1 AND 3")
        :is
        "SELECT * FROM hivetbl WHERE 0 NOT BETWEEN 1 AND 3"
    )
    (:fact to-hive:where:predicate:like
        (sql->hive context "SELECT * FROM tbl WHERE 'a' LIKE 'a'")
        :is
        "SELECT * FROM hivetbl WHERE 'a' LIKE 'a'"
    )
    (:fact to-hive:where:predicate:like:not
        (sql->hive context "SELECT * FROM tbl WHERE 'a' NOT LIKE 'a'")
        :is
        "SELECT * FROM hivetbl WHERE 'a' NOT LIKE 'a'"
    )
    (:fact to-hive:where:predicate:reglike
        (sql->hive context "SELECT * FROM tbl WHERE 'a' REGEXP 'a'")
        :is
        "SELECT * FROM hivetbl WHERE 'a' REGEXP 'a'"
    )
    (:fact to-hive:where:predicate:reglike:not
        (sql->hive context "SELECT * FROM tbl WHERE 'a' NOT REGEXP 'b'")
        :is
        "SELECT * FROM hivetbl WHERE 'a' NOT REGEXP 'b'"
    )
    (:fact to-hive:where:simple-expr:binary
        (sql->hive context "SELECT * FROM tbl WHERE BINARY 'a' <> 'A'")
        :is
        "SELECT * FROM hivetbl WHERE BINARY('a') <> 'A'"
    )
    (:fact to-hive:where:simple-expr:exists
        (sql->hive context "SELECT * FROM tbl WHERE EXISTS (SELECT * FROM tbl)")
        :is
        "SELECT * FROM hivetbl WHERE EXISTS (SELECT * FROM hivetbl)"
    )
    (:fact to-hive:where:cast:int
        (sql->hive context "SELECT * FROM tbl WHERE cast('1' as int)=1")
        :is
        "SELECT * FROM hivetbl WHERE CAST('1' AS INT) = 1"
    )
    (:fact to-hive:where:case
        (sql->hive context "SELECT * FROM tbl WHERE case when TRUE then FALSE end")
        :is
        "SELECT * FROM hivetbl WHERE CASE WHEN TRUE THEN FALSE END"
    )
    (:fact to-hive:where:case:else
        (sql->hive context "SELECT * FROM tbl WHERE case when 1 then 2 else 3 end")
        :is
        "SELECT * FROM hivetbl WHERE CASE WHEN 1 THEN 2 ELSE 3 END"
    )
    (:fact to-hive:where:case:value
        (sql->hive context "SELECT col FROM tbl WHERE case col when 1 then TRUE when 0 then FALSE end")
        :is
        "SELECT col FROM hivetbl WHERE CASE col WHEN 1 THEN TRUE WHEN 0 THEN FALSE END"
    )
    (:fact to-hive:select:distinct-count:1
        (sql->hive context "SELECT count(distinct col) FROM tbl")
        :is
        "SELECT COUNT(DISTINCT col) FROM hivetbl"
    )
    (:fact to-hive:select:distinct-count:2
        (sql->hive context "SELECT count(distinct col, 1) FROM tbl")
        :is
        "SELECT COUNT(DISTINCT col, 1) FROM hivetbl"
    )
    (:fact to-hive:select:distinct-sum:1
        (sql->hive context "SELECT sum(distinct col) FROM tbl")
        :is
        "SELECT SUM(DISTINCT col) FROM hivetbl"
    )
    (:fact to-hive:select:distinct-avg:1
        (sql->hive context "SELECT avg(distinct col) FROM tbl")
        :is
        "SELECT AVG(DISTINCT col) FROM hivetbl"
    )
    (:fact to-hive:where:pow
        (sql->hive context "SELECT * FROM tbl WHERE pow(1, 0)=1")
        :is
        "SELECT * FROM hivetbl WHERE POWER(1, 0) = 1"
    )
    (:fact to-hive:where:power
        (sql->hive context "SELECT * FROM tbl WHERE power(1, 0)=1")
        :is
        "SELECT * FROM hivetbl WHERE POWER(1, 0) = 1"
    )
    (:fact to-hive:where:rand:0
        (sql->hive context "SELECT * FROM tbl WHERE rand()=1")
        :is
        "SELECT * FROM hivetbl WHERE RAND() = 1"
    )
    (:fact to-hive:where:e:0
        (sql->hive context "SELECT * FROM tbl WHERE e()<>0")
        :is
        "SELECT * FROM hivetbl WHERE E() <> 0"
    )
    (:fact to-hive:select:from_unixtime:1
        (sql->hive context "SELECT from_unixtime(col) FROM tbl")
        :is
        "SELECT FROM_UNIXTIME(col) FROM hivetbl"
    )
)

(def viewed-context {
    :ns [{
        :type "namespace"
        :name "app"
        :children [{
            :type "view"
            :name "vw"
            :hive-name "hiveview"
            :children [
                {
                    :name "cc"
                }
            ]
        }]
    }]
    :default-ns ["app"]
})

(def ctas-context {
    :ns [{
        :type "namespace"
        :name "app"
        :children [{
            :type "ctas"
            :name "ctas"
            :hive-name "hivectas"
            :children [
                {
                    :name "cc"
                }
            ]
        }]
    }]
    :default-ns ["app"]
})

(defn- insert-view' [ty nz default-nz view-name]
    (if (empty? default-nz)
        (conj (:children nz) {
            :type (case ty :create-view "view" :create-ctas "ctas")
            :name view-name
            :hive-name (case ty :create-view "hiveview" :create-ctas "hivectas")
        })
        (let [
            [x & xs] default-nz
            new-nz (for [c nz]
                (if-not (= (:name c) x)
                    c
                    (assoc c :children (insert-view' ty (:children c) xs view-name))
                )
            )
            ]
            new-nz
        )
    )
)

(defn- insert-view [context dfg]
{
    :pre [
        (#{:create-view :create-ctas} (:type dfg))
    ]
}
    (let [
        view-refer (:value (:name dfg))
        view-nz (drop-last view-refer)
        view-name (last view-refer)
        default-nz (:default-ns context)
        default-nz-prefix (drop-last (count view-nz) default-nz)
        nz (insert-view' (:type dfg) (:ns context)
            (concat default-nz-prefix view-nz)
            view-name
        )
        ]
        {
            :default-ns (:default-ns context)
            :ns nz
        }
    )
)

(suite "view"
    (:fact view:create
        (let [
            dfg (trans/parse-sql context "CREATE VIEW vw AS select * from tbl")
            viewed-context (insert-view context dfg)
            r (trans/dump-hive viewed-context dfg)
            ]
            r
        )
        :is
        "CREATE VIEW hiveview AS SELECT * FROM hivetbl"
    )
    (:fact view:create:ns
        (let [
            dfg (trans/parse-sql context "CREATE VIEW ver.vw AS select * from tbl")
            viewed-context (insert-view context dfg)
            r (trans/dump-hive viewed-context dfg)
            ]
            r
        )
        :is
        "CREATE VIEW hiveview AS SELECT * FROM hivetbl"
    )
    (:fact view:create:col
        (let [
            dfg (trans/parse-sql context "CREATE VIEW vw(cc) AS select * from tbl")
            viewed-context (insert-view context dfg)
            r (trans/dump-hive viewed-context dfg)
            ]
            r
        )
        :is
        "CREATE VIEW hiveview(cc) AS SELECT * FROM hivetbl"
    )
    (:fact select:from:view
        (sql->hive viewed-context "SELECT * FROM vw WHERE vw.cc<>0")
        :is
        "SELECT * FROM hiveview WHERE hiveview.cc <> 0"
    )
    (:fact drop:view
        (sql->hive viewed-context "DROP VIEW vw")
        :is
        "DROP VIEW hiveview"
    )
    (:fact drop:view:ns
        (sql->hive viewed-context "DROP VIEW app.vw")
        :is
        "DROP VIEW hiveview"
    )
    (:fact create:ctas
        (let [
            dfg (trans/parse-sql context "CREATE TABLE ctas AS select * from tbl")
            viewed-context (insert-view context dfg)
            r (trans/dump-hive viewed-context dfg)
            ]
            r
        )
        :is
        "CREATE TABLE hivectas AS SELECT * FROM hivetbl"
    )
    (:fact create:ctas:ns
        (let [
            dfg (trans/parse-sql context "CREATE TABLE ver.ctas AS select * from tbl")
            viewed-context (insert-view context dfg)
            r (trans/dump-hive viewed-context dfg)
            ]
            r
        )
        :is
        "CREATE TABLE hivectas AS SELECT * FROM hivetbl"
    )
    (:fact drop:ctas
        (sql->hive ctas-context "DROP TABLE ctas")
        :is
        "DROP TABLE hivectas"
    )
    (:fact drop:ctas:ns
        (sql->hive ctas-context "DROP TABLE app.ctas")
        :is
        "DROP TABLE hivectas"
    )
)
