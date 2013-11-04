(ns parser.translator
    (:require
        [clojure.string :as str]
        [utilities.parse :as prs]
        [parser.sql-2003 :as sql]
    )
    (:use
        [clojure.set :only (union)]
        [logging.core :only (defloggers)]
    )
    (:import
        utilities.parse.InvalidSyntaxException
    )
)

(defloggers debug info warn error)

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

(defn search-table [nss refered]
{
    :pre [(>= (count refered) 1)]
}
    (let [[x & xs] refered]
        (if-let [nz (search-name-in-ns nss x)]
            (if (empty? xs)
                nz
                (if (= (:type nz) "namespace")
                    (recur (:children nz) xs)
                )
            )
        )
    )
)

(defn normalize-table' [nss nz refered]
    (let [
        x (concat nz refered)
        r (search-table nss x)
        ]
        (if (= (:type r) "table")
            r
            (if-not (empty? nz)
                (recur nss (drop-last nz) refered)
            )
        )
    )
)

(defn normalize-table [context refered]
    (if-let [res (normalize-table' (:ns context) (:default-ns context) refered)]
        res
        (throw (InvalidSyntaxException.
            (format "unknown table: %s" (pr-str refered))
        ))
    )
)


(declare analyze-sql:value-expr)

(defn- conflict-alias [old new]
    (throw (InvalidSyntaxException. "duplicated aliases"))
)

(defn- analyze-sql:within-from [context ast]
    (condp contains? (:type ast)
        #{:table} (let [
            ext-tbl (->> ast (:refer) (normalize-table context))
            nm (:name ast)
            tbl-alias (cond
                (not (nil? nm)) [nm]
                :else (:refer ast)
            )
            res (assoc ast :refer ext-tbl)
            ]
            [res {tbl-alias res}]
        )
        #{:derived-table} (let [
            nm (:name ast)
            query (->> ast (:value) (analyze-sql:value-expr context))
            res (assoc ast :value query :name nm)
            ]
            [res {[nm] res}]
        )
        #{:join, :cross-join, :outer-join} (let [
            [left left-aliases] (->> ast
                (:left)
                (analyze-sql:within-from context)
            )
            [right right-aliases] (->> ast
                (:right)
                (analyze-sql:within-from context)
            )
            final-aliases (merge-with conflict-alias left-aliases right-aliases)
            on-cond (->> ast
                (:on)
                (analyze-sql:value-expr
                    (assoc context :table-aliases final-aliases)
                )
            )
            join-type (:join-type ast)
            res {
                :type (:type ast)
                :left left
                :right right
            }
            res (if-not join-type res (assoc res :join-type join-type))
            res (if-not on-cond res (assoc res :on on-cond))
            ]
            [res final-aliases]
        )
    )
)

(defn- analyze-sql:from-clause [context from-clause]
{
    :pre [(sequential? from-clause)]
}
    (let [
        res (for [x from-clause]
            (analyze-sql:within-from context x)
        )
        new-from-clause (map first res)
        aliases (map second res)
        aliases (apply merge-with conflict-alias aliases)
        ]
        [new-from-clause (assoc context :table-aliases aliases)]
    )
)

(defn analyze-sql:select-list [context sl]
{
    :pre [(= (:type sl) :derived-column)]
}
    (let [
        v (:value sl)
        new-v (analyze-sql:value-expr context v)
        ]
        (assoc sl :value new-v)
    )
)

(defn analyze-sql:select [context ast]
{
    :pre [(= (:type ast) :select)
        (:select-list ast)
        (:from-clause ast)
    ]
}
    (let [
        [from-clause context-with-table-aliases] (->> ast
            (:from-clause)
            (analyze-sql:from-clause context)
        )
        select-list (->> ast
            (:select-list)
            (map (partial analyze-sql:select-list context-with-table-aliases))
        )
        where (->> ast
            (:where)
            (analyze-sql:value-expr context-with-table-aliases)
        )
        group-by (->> ast
            (:group-by)
            (map (partial analyze-sql:value-expr context-with-table-aliases))
        )
        order-by (->> ast
            (:order-by)
            (map (partial analyze-sql:value-expr context-with-table-aliases))
        )
        dfg (assoc ast
            :select-list select-list
            :from-clause from-clause
        )
        dfg (if-not where dfg (assoc dfg :where where))
        dfg (if (empty? group-by) dfg (assoc dfg :group-by group-by))
        dfg (if (empty? order-by) dfg (assoc dfg :order-by order-by))
        ]
        dfg
    )
)

(defn- analyze-sql:value-expr [context ast]
    (cond
        (nil? ast) nil
        (sequential? ast) (map
            (partial analyze-sql:value-expr context)
            ast
        )
        (= (:type ast) :select) (analyze-sql:select
            context ast
        )
        (= (:type ast) :asterisk) (if-let [
            rname (:refer ast)
            ]
            (if-let [r ((:table-aliases context) rname)]
                (assoc ast :refer r)
                (throw (InvalidSyntaxException.
                    (format "unknown table name: %s" rname)
                ))
            )
            ast
        )
        (= (:type ast) :dotted-identifier) (let [
            v (:value ast)
            t (drop-last v)
            r (if (empty? t)
                nil
                (if-let [x ((:table-aliases context) t)]
                    x
                    (throw (InvalidSyntaxException.
                        (format "unknown table name: %s" (str/join "." t))
                    ))
                )
            )
            res {:type :identifier, :value (last v)}
            res (if-not r res (assoc res :refer r))
            ]
            res
        )
        (map? ast) (into {}
            (for [[k v] ast]
                [k (analyze-sql:value-expr context v)]
            )
        )
        :else ast
    )
)

(defn analyze-sql [context ast]
    (analyze-sql:value-expr context ast)
)


(declare dump-hive:value-expr)
(declare dump-hive:value-subexpr)

(defn- dump-hive:select-list [dfg]
    (str/join ", "
        (for [
            s (:select-list dfg)
            :let [_ (assert (= (:type s) :derived-column))]
            :let [v (:value s)]
            :let [n (:name s)]
            :let [col (dump-hive:value-subexpr v)]
            ]
            (cond
                (nil? n) col
                (sequential? n) (format "%s (%s)" col
                    (str/join ", "
                        (map dump-hive:value-expr n)
                    )
                )
                :else (format "%s %s" col (dump-hive:value-expr n))
            )
        )
    )
)

(defn- dump-hive:table [dfg]
    (condp contains? (:type dfg)
        #{:derived-table} (let [
            nm (:name dfg)
            exp (->> dfg
                (:value)
                (dump-hive:value-subexpr)
            )
            ]
            (format "%s %s" exp (quoted \` nm))
        )
        #{:table} (let [
            tbl (->> dfg
                (:refer)
                (:hive-name)
            )
            nm (:name dfg)
            ]
            (if nm
                (format "%s %s" tbl (quoted \` nm))
                tbl
            )
        )
        #{:cross-join :join :outer-join} (let [
            left (dump-hive:table (:left dfg))
            right (dump-hive:table (:right dfg))
            join-cond (:on dfg)
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
                (format "%s %s %s ON %s" left join-str right
                    (dump-hive:value-subexpr join-cond)
                )
            )
        )
    )
)


(defn- dump-hive:from-clause [dfg]
    (str/join ", "
        (for [tbl (:from-clause dfg)]
            (dump-hive:table tbl)
        )
    )
)

(defn- dump-hive:where [dfg]
    (when-let [w (:where dfg)]
        (dump-hive:value-expr w)
    )
)

(defn- dump-hive:group-by [dfg]
    (when-let [g (:group-by dfg)]
        (str/join ", "
            (for [x g]
                (dump-hive:value-expr x)
            )
        )
    )
)

(defn- dump-hive:order-by [dfg]
    (when-let [orders (:order-by dfg)]
        (str/join ", "
            (for [
                x orders
                :let [expr (:value x)]
                :let [ordering (:ordering x)]
                ]
                (format "%s%s"
                    (dump-hive:value-expr expr)
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

(defn- dump-hive:limit [dfg]
    (when-let [limit (:limit dfg)]
        (dump-hive:value-expr limit)
    )
)

(defn- dump-hive:select [dfg]
    (let [
        from-clause (dump-hive:from-clause dfg)
        select-list (dump-hive:select-list dfg)
        where (dump-hive:where dfg)
        group-by (dump-hive:group-by dfg)
        order-by (dump-hive:order-by dfg)
        limit (dump-hive:limit dfg)
        hive (case (:set-quantifier dfg)
            :all (format "SELECT ALL %s FROM %s" select-list from-clause)
            :distinct (format "SELECT DISTINCT %s FROM %s" select-list from-clause)
            nil (format "SELECT %s FROM %s" select-list from-clause)
        )
        hive (if-not where hive (format "%s WHERE %s" hive where))
        hive (if-not group-by hive (format "%s GROUP BY %s" hive group-by))
        hive (if-not order-by hive (format "%s ORDER BY %s" hive order-by))
        hive (if-not limit hive (format "%s LIMIT %s" hive limit))
        ]
        hive
    )
)

(defn- dump-hive:format-args [args]
    (str/join ", "
        (map dump-hive:value-subexpr args)
    )
)

(defn- dump-hive:unary-op [dfg]
    (let [
        v (->> dfg
            (:value)
            (dump-hive:value-subexpr)
        )
        op (case (:type dfg)
            :boolean-negation "NOT"
            :unary+ "+"
            :unary- "-"
            :unary-tilde "~"
            :exists "EXISTS"
        )
        ]
        (format "%s %s" op v)
    )
)

(defn- dump-hive:binary-op [dfg]
    (let [
        left (dump-hive:value-subexpr (:left dfg))
        right (dump-hive:value-subexpr (:right dfg))
        connective (case (:type dfg)
            :is "IS"
            :is-not "IS NOT"
            :like "LIKE"
            :not-like "NOT LIKE"
            :reglike "REGEXP"
            :not-reglike "NOT REGEXP"
            (name (:type dfg))
        )
        ]
        (format "%s %s %s" left connective right)
    )
)

(defn- dump-hive:value-subexpr [dfg]
    (let [res (dump-hive:value-expr dfg)]
        (if (contains? #{
                    :numeric-literal :hex-string-literal :date-literal
                    :interval-literal :national-string-literal
                    :character-string-literal :boolean-literal :identifier
                    :dotted-identifier :null-literal :asterisk :binary :cast
                    :distinct-count :distinct-sum :distinct-avg :func-call
                }
                (:type dfg)
            )
            res
            (format "(%s)" res)
        )
    )
)

(defn dump-hive:value-expr [dfg]
    (condp contains? (:type dfg)
        #{:value-expr} (str/join " "
            (for [x (:value dfg)]
                (case x
                    :and "AND"
                    :or "OR"

                    (dump-hive:value-subexpr x)
                )
            )
        )

        #{:bit-expr} (str/join " "
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

                    (dump-hive:value-subexpr x)
                )
            )
        )

        #{:identifier} (let [
            res (->> dfg
                (:value)
                (quoted \`)
            )
            refr (:refer dfg)
            refr (if refr (or
                (:name refr)
                (->> refr (:refer) (:hive-name))
            ))
            ]
            (if-not refr
                res
                (format "%s.%s" refr res)
            )
        )

        #{:asterisk} (let [r (:refer dfg)]
            (cond
                (nil? r) "*"
                (= (:type r) :table) (let [
                    nm (or (:name r) (->> r (:refer) (:hive-name)))
                    ]
                    (format "%s.*" nm)
                )
            )
        )

        #{:null-literal} "NULL"

        #{:numeric-literal :boolean-literal :hex-string-literal :date-literal
            :time-literal :timestamp-literal :interval-literal
            :national-string-literal :character-string-literal
        }
        (:value dfg)

        #{:binary} (let [
            v (:value dfg)
            ]
            ; Hive does not support BINARY operator, but it has a BINARY function
            (format "BINARY(%s)" (dump-hive:value-expr v))
        )

        #{:boolean-negation :unary+ :unary- :unary-tilde :exists}
        (dump-hive:unary-op dfg)

        #{:is :is-not :<=> :< :> :<= :>= :<> := :like :not-like :reglike
            :not-reglike
        }
        (dump-hive:binary-op dfg)

        #{:in-array :not-in-array} (let [
            l (:left dfg)
            r (:right dfg)
            nop (if (= (:type dfg) :not-in-array) " NOT " " ")
            ]
            (format "%s%sIN (%s)"
                (dump-hive:value-subexpr l)
                nop
                (dump-hive:format-args r)
            )
        )

        #{:cast} (let [
            l (:left dfg)
            r (:right dfg)
            ]
            (format "CAST(%s AS %s)"
                (dump-hive:value-subexpr l)
                (->> r (name) (str/upper-case))
            )
        )

        #{:between :not-between} (let [
            l (:left dfg)
            m (:middle dfg)
            r (:right dfg)
            nop (if (= (:type dfg) :not-between) " NOT " " ")
            ]
            (format "%s%sBETWEEN %s AND %s"
                (dump-hive:value-subexpr l)
                nop
                (dump-hive:value-subexpr m)
                (dump-hive:value-subexpr r)
            )
        )

        #{:select} (->> dfg (dump-hive:select))

        #{:union} (do
            (assert (= (:qualifier dfg) :all))
            (str/join " UNION ALL "
                (map dump-hive:value-subexpr (:selects dfg))
            )
        )


        #{:case} (let [
            v (:value dfg)
            ww (:when dfg)
            ww (str/join " "
                (for [[x y] ww]
                    (format "WHEN %s THEN %s"
                        (dump-hive:value-subexpr x)
                        (dump-hive:value-subexpr y)
                    )
                )
            )
            else (:else dfg)
            res "CASE"
            res (if-not v res (format "%s %s" res (dump-hive:value-subexpr v)))
            res (format "%s %s" res ww)
            res (if-not else
                res
                (format "%s ELSE %s" res (dump-hive:value-subexpr else))
            )
            res (format "%s END" res)
            ]
            res
        )
        #{:distinct-count :distinct-sum :distinct-avg} (let [
            op (case (:type dfg)
                :distinct-count "COUNT"
                :distinct-sum "SUM"
                :distinct-avg "AVG"
            )
            args (dump-hive:format-args (:args dfg))
            ]
            (format "%s(DISTINCT %s)" op args)
        )
        #{:func-call} (let [
            f (:func dfg)
            args (:args dfg)
            ]
            (format "%s(%s)"
                (-> f (name) (str/upper-case))
                (dump-hive:format-args args)
            )
        )
    )
)

(defn dump-hive [context dfg]
    (let [
        dfg (case (:type dfg)
            dfg
        )
        hive (dump-hive:value-expr dfg)
        ]
        (info "dump hive" :hive hive)
        hive
    )
)

(defn parse-sql [context sql-text]
    (info "start parsing" :sql sql-text :context (str context))
    (let [
        ast (sql/sql sql-text)
        dfg (analyze-sql context ast)
        ]
        dfg
    )
)
