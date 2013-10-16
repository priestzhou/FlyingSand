(ns parser.translator
    (:require
        [clojure.string :as str]
        [utilities.parse :as prs]
        [parser.sql-2003 :as sql]
    )
    (:use
        [clojure.set :only (union)]
    )
    (:import 
        utilities.parse.InvalidSyntaxException
    )
)

(defn regular-identifier? [stream]
    (let [
        id-start (conj prs/letter \_)
        id-body (union id-start prs/digit)
        [_ prsd1] (->> stream
            ((prs/optional
                (prs/chain
                    (prs/expect-char-if id-start)
                    (prs/many
                        (prs/expect-char-if id-body)
                    )
                    (prs/expect-eof)
                )
            ))
        )
        [_ prsd2] (->> stream
            ((prs/optional
                (prs/chain
                    sql/regular-identifier
                    (prs/expect-eof)
                )
            ))
        )
        ]
        (and prsd1 prsd2)
    )
)

(defn quoted [mark id]
    (let [
        regular? (->> id
            (prs/str->stream)
            (regular-identifier?)
        )
        ]
        (if regular?
            id
            (let [
                escaped (apply concat
                    (for [x id]
                        (if (= x mark)
                            [x x]
                            [x]
                        )
                    )
                )
                escaped (str/join "" escaped)
                ]
                (format "%s%s%s" mark escaped mark)
            )
        )
    )
)

(declare analyze-sql:value-expr)

(defn search-name-in-ns [nss nm]
    (first 
        (for [
            sub nss
            :when (= nm (:name sub))
            ]
            sub
        )
    )
)

(defn table? [nss refered]
    (if (= 1 (count refered))
        (let [tbl (search-name-in-ns nss (first refered))]
            (= (:type tbl) "table")
        )
        (let [[x & xs] refered]
            (if-let [nz (search-name-in-ns nss x)]
                (if (= (:type nz) "namespace")
                    (recur (:children nz) xs)
                    false
                )
                false
            )
        )
    )
)

(defn normalize-table' [nss nz refered]
    (let [x (concat nz refered)]
        (if (table? nss x)
            x
            (if (empty? nz)
                (throw (InvalidSyntaxException. 
                    (format "unknown table: %s" (pr-str refered))
                ))
                (recur nss (drop-last nz) refered)
            )
        )
    )
)

(defn normalize-table [context refered]
    (normalize-table' (:ns context) (:default-ns context) refered)
)

(declare translate-table)

(defn translate-raw-table [context tbl]
    (let [
        refer-tbl (->> tbl (:refer) (normalize-table context))
        ]
        (-> tbl
            (assoc :type :external-table, :refer refer-tbl)
        )
    )

)

(defn translate-join [context tbl]
    (assoc tbl
        :left (translate-table context (:left tbl))
        :right (translate-table context (:right tbl))
    )
)

(defn translate-table [context tbl]
    (let [
        nm (:name tbl)
        res (cond
            (:query tbl) (analyze-sql:value-expr context (:query tbl))
            :else (case (:type tbl)
                :table (translate-raw-table context tbl)
                :cross-join (translate-join context tbl)
                :join (translate-join context tbl)
                :outer-join (translate-join context tbl)
            )
        )
        res (if-not nm res (assoc res :name nm))
        ]
        res
    )
)

(defn translate-from-clause [context from-clause]
    (for [
        tbl from-clause
        ]
        (translate-table context tbl)
    )
)

(defn analyze-sql:select [context ast]
    (let [
        from-clause (->> ast
            (:from-clause)
            (translate-from-clause context)
        )
        dfg (assoc ast :from-clause from-clause)
        ]
        dfg
    )
)

(defn analyze-sql:union [context ast]
    (let [
        selects (map (partial analyze-sql:select context) (:selects ast))
        ]
        (assoc ast :selects selects)
    )
)

(defn analyze-sql:value-expr [context ast]
    (case (:type ast)
        :select (analyze-sql:select context ast)
        :union (analyze-sql:union context ast)
    )
)

(defn analyze-sql [context ast]
    (analyze-sql:value-expr context ast)
)


(declare dump-hive:value-expr)
(declare dump-hive:value-subexpr)

(defn dump-hive:select-list [context dfg]
    (let [select-list (:select-list dfg)]
        (cond
            (= select-list :asterisk) "*"
            :else (str/join ", "
                (for [s select-list]
                    (case (:type s)
                        :derived-column (let [
                            v (:value s)
                            ]
                            (case (:type v)
                                :dotted-identifier (->> v
                                    (:value)
                                    (map (partial quoted \`))
                                    (str/join ".")
                                )
                            )
                        )
                    )
                )
            )
        )
    )
)

(declare dump-hive:table)

(defn dump-hive:join [context dfg]
    (let [
        left (dump-hive:table context (:left dfg))
        right (dump-hive:table context (:right dfg))
        join-cond (if (:on dfg) (dump-hive:value-subexpr context (:on dfg)))
        join-str (case (:type dfg)
            :cross-join "CROSS JOIN"
            :join "JOIN"
            :outer-join (format "%s JOIN" 
                (case (:join-type dfg)
                    :left "LEFT"
                    :right "RIGHT"
                    :full "FULL"
                )
            )
        )
        ]
        (if-not join-cond
            (format "%s %s %s" left join-str right)
            (format "%s %s %s ON %s" left join-str right join-cond)
        )
    )
)

(defn dump-hive:table [context dfg]
    (case (:type dfg)
        :select (let [
            nm (:name dfg)
            exp (->> dfg
                (dump-hive:value-subexpr context)
            )
            ]
            (format "%s %s" exp (quoted \` nm))
        )
        :external-table (let [
            nm (:name dfg)
            tbl (->> dfg
                (:refer)
                ((:name-conversion context))
            )
            nm (if nm 
                nm
                (->> dfg
                    (:refer)
                    (last)
                )
            )
            ]
            (format "%s %s" tbl (quoted \` nm))
        )
        :cross-join (dump-hive:join context dfg)
        :join (dump-hive:join context dfg)
        :outer-join (dump-hive:join context dfg)
    )
)


(defn dump-hive:from-clause [context dfg]
    (str/join ", "
        (for [
            tbl (:from-clause dfg)
            ]
            (dump-hive:table context tbl)
        )
    )
)

(defn dump-hive:where [context dfg]
    (when-let [w (:where dfg)]
        (dump-hive:value-expr context w)
    )
)

(defn dump-hive:group-by [context dfg]
    (when-let [g (:group-by dfg)]
        (str/join ", "
            (for [x g]
                (dump-hive:value-expr context x)
            )
        )
    )
)

(defn dump-hive:order-by [context dfg]
    (when-let [orders (:order-by dfg)]
        (str/join ", "
            (for [
                x orders
                :let [expr (:value x)]
                :let [ordering (:ordering x)]
                ]
                (format "%s%s"
                    (dump-hive:value-expr context expr)
                    (case ordering
                        :asc " ASC"
                        :desc " DESC"
                        nil ""
                    )
                )
            )
        )
    )
)

(defn dump-hive:limit [context dfg]
    (when-let [limit (:limit dfg)]
        (dump-hive:value-expr context limit)
    )
)

(defn dump-hive:select [context dfg]
    (let [
        from-clause (dump-hive:from-clause context dfg)
        select-list (dump-hive:select-list context dfg)
        where (dump-hive:where context dfg)
        group-by (dump-hive:group-by context dfg)
        order-by (dump-hive:order-by context dfg)
        limit (dump-hive:limit context dfg)
        hive (format "SELECT %s FROM %s" select-list from-clause)
        hive (if-not where hive (format "%s WHERE %s" hive where))
        hive (if-not group-by hive (format "%s GROUP BY %s" hive group-by))
        hive (if-not order-by hive (format "%s ORDER BY %s" hive order-by))
        hive (if-not limit hive (format "%s LIMIT %s" hive limit))
        ]
        hive
    )
)

(defn dump-hive:test [context dfg]
    (let [
        left (dump-hive:value-subexpr context (:left dfg))
        connective (case (:connective dfg)
            :is "IS"
            :is-not "IS NOT"
        )
        right (dump-hive:value-expr context (:right dfg))
        ]
        (format "%s %s %s" left connective right)
    )
)

(defn dump-hive:value-subexpr [context dfg]
    (let [res (dump-hive:value-expr context dfg)]
        (if (contains? 
                #{
                :numeric-literal :hex-string-literal :date-literal
                :interval-literal :national-string-literal :character-string-literal
                :boolean-literal :identifier :dotted-identifier
                :null-literal
                }
                (:type dfg)
            )
            res
            (format "(%s)" res)
        )
    )
)

(defn dump-hive:binary [context dfg]
    (let [
        left (dump-hive:value-subexpr context (:left dfg))
        right (dump-hive:value-subexpr context (:right dfg))
        connective (case (:type dfg)
            :is "IS"
            :is-not "IS NOT"
            (name (:type dfg))
        )
        ]
        (format "%s %s %s" left connective right)
    )
)

(defn dump-hive:value-expr [context dfg]
    (case (:type dfg)
        :value-expr (str/join " "
            (for [x (:value dfg)]
                (case x
                    :and "AND"
                    :or "OR"

                    (dump-hive:value-subexpr context x)
                )
            )
        )
        :bit-expr (str/join " "
            (for [x (:value dfg)]
                (case x
                    :| "|"
                    :& "&"
                    :<< "<<"
                    :>> ">>"

                    :+ "+"
                    :- "-"
                    :* "*"
                    :div "/"
                    :mod "%"
                    :caret "^"

                    (dump-hive:value-subexpr context x)
                )
            )
        )
        :identifier (->> dfg
            (:value)
            (quoted \`)
        )
        :dotted-identifier (->> dfg
            (:value)
            (map (partial quoted \`))
            (str/join ".")
        )
        :null-literal "NULL"
        :numeric-literal (:value dfg)
        :boolean-literal (:value dfg)
        :hex-string-literal (:value dfg)
        :date-literal (:value dfg)
        :time-literal (:value dfg)
        :timestamp-literal (:value dfg)
        :interval-literal (:value dfg)
        :national-string-literal (:value dfg)
        :character-string-literal (:value dfg)
        :boolean-negation (->> dfg
            (:value)
            (dump-hive:value-subexpr context)
            (format "NOT %s")
        )
        :unary+ (->> dfg
            (:value)
            (dump-hive:value-subexpr context)
            (format "+ %s")
        )
        :unary- (->> dfg
            (:value)
            (dump-hive:value-subexpr context)
            (format "- %s")
        )
        :unary-tilde (->> dfg
            (:value)
            (dump-hive:value-subexpr context)
            (format "~ %s")
        )
        :is (dump-hive:binary context dfg)
        :is-not (dump-hive:binary context dfg)
        :<=> (dump-hive:binary context dfg)
        :< (dump-hive:binary context dfg)
        :> (dump-hive:binary context dfg)
        :<= (dump-hive:binary context dfg)
        :>= (dump-hive:binary context dfg)
        :<> (dump-hive:binary context dfg)
        := (dump-hive:binary context dfg)
        :in-array (let [
            l (:left dfg)
            r (:right dfg)
            ]
            (format "%s IN (%s)" 
                (dump-hive:value-subexpr context l)
                (str/join ", "
                    (for [x r]
                        (dump-hive:value-expr context x)
                    )
                )
            )
        )
        :not-in-array (let [
            l (:left dfg)
            r (:right dfg)
            ]
            (format "%s NOT IN (%s)" 
                (dump-hive:value-subexpr context l)
                (str/join ", "
                    (for [x r]
                        (dump-hive:value-expr context x)
                    )
                )
            )
        )
        :like (let [
            l (:left dfg)
            r (:right dfg)
            ]
            (format "%s LIKE %s"
                (dump-hive:value-subexpr context l)
                (dump-hive:value-subexpr context r)
            )
        )
        :not-like (let [
            l (:left dfg)
            r (:right dfg)
            ]
            (format "%s NOT LIKE %s"
                (dump-hive:value-subexpr context l)
                (dump-hive:value-subexpr context r)
            )
        )
        :reglike (let [
            l (:left dfg)
            r (:right dfg)
            ]
            (format "%s REGEXP %s"
                (dump-hive:value-subexpr context l)
                (dump-hive:value-subexpr context r)
            )
        )
        :not-reglike (let [
            l (:left dfg)
            r (:right dfg)
            ]
            (format "%s NOT REGEXP %s"
                (dump-hive:value-subexpr context l)
                (dump-hive:value-subexpr context r)
            )
        )
        :between (let [
            l (:left dfg)
            m (:middle dfg)
            r (:right dfg)
            ]
            (format "%s BETWEEN %s AND %s"
                (dump-hive:value-subexpr context l)
                (dump-hive:value-subexpr context m)
                (dump-hive:value-subexpr context r)
            )
        )
        :not-between (let [
            l (:left dfg)
            m (:middle dfg)
            r (:right dfg)
            ]
            (format "%s NOT BETWEEN %s AND %s"
                (dump-hive:value-subexpr context l)
                (dump-hive:value-subexpr context m)
                (dump-hive:value-subexpr context r)
            )
        )
        :select (->> dfg
            (dump-hive:select context)
        )
        :union (do
            (assert (= (:qualifier dfg) :all))
            (str/join " UNION ALL "
                (map (partial dump-hive:value-subexpr context) (:selects dfg))
            )
        )
    )
)

(defn dump-hive [context dfg]
    (dump-hive:value-expr context dfg)
)

(defn sql-2003->hive [context sql-text]
    (let [
        [_ ast] (->> sql-text
            (prs/str->stream)
            (prs/positional-stream)
            ((prs/choice*
                second (prs/chain
                    sql/blank*
                    sql/query
                    sql/blank*
                    (prs/expect-eof)
                )
            ))
        )
        _ (prn :ast ast)
        dfg (analyze-sql context ast)
        _ (prn :dfg dfg)
        hive (dump-hive context dfg)
        ]
        (prn :hive hive)
        hive
    )
)
