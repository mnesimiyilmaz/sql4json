package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.BoundParameters;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.parser.*;
import io.github.mnesimiyilmaz.sql4json.registry.*;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.SqlDateTime;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Walks a {@link QueryDefinition} and replaces every {@link Expression.ParameterRef} with
 * a resolved {@link Expression.LiteralVal}. Returns a new immutable {@code QueryDefinition}.
 *
 * <p>Thread-safe: stateless, per-call construction of the substituted definition. The
 * original {@code QueryDefinition} is never mutated.
 */
public final class ParameterSubstitutor {

    private ParameterSubstitutor() {
        // Utility class — not instantiable.
    }

    /**
     * Returns a {@link QueryDefinition} with every {@code ParameterRef} replaced by a
     * resolved literal. Walks SELECT, WHERE, GROUP BY, HAVING, ORDER BY, and LIMIT / OFFSET.
     *
     * <p>Queries that declared no placeholders are returned unchanged (fast-path) so repeat
     * execution avoids any re-resolution cost.
     *
     * @param def      the parsed query definition (may contain {@code ParameterRef} nodes)
     * @param params   the bound parameter values
     * @param settings the settings (used to look up the {@link ConditionHandlerRegistry} for
     *                 re-resolving substituted condition contexts)
     * @return a substituted {@code QueryDefinition}
     * @throws SQL4JsonBindException on any bind-time error (missing / extra / unsupported type)
     */
    public static QueryDefinition substitute(QueryDefinition def, BoundParameters params,
                                             Sql4jsonSettings settings) {
        boolean hasParams = def.positionalCount() > 0
                || !def.namedParameters().isEmpty()
                || def.limitParam() != null
                || def.offsetParam() != null;
        if (!hasParams) {
            return def;
        }

        var registry = ConditionHandlerRegistry.forSettings(settings);

        List<SelectColumnDef> newSelects = def.selectedColumns().stream()
                .map(sc -> substituteSelectColumn(sc, params, settings))
                .toList();

        List<Expression> newGroupBy = def.groupBy() == null
                ? null
                : def.groupBy().stream()
                  .map(e -> substituteExpression(e, params, settings))
                  .toList();

        List<OrderByColumnDef> newOrderBy = def.orderBy() == null
                ? null
                : def.orderBy().stream()
                  .map(o -> OrderByColumnDef.of(
                          substituteExpression(o.expression(), params, settings),
                          o.direction()))
                  .toList();

        CriteriaNode newWhere = substituteCriteria(def.whereClause(), params, settings, registry);
        CriteriaNode newHaving = substituteCriteria(def.havingClause(), params, settings, registry);

        Integer newLimit = def.limit();
        Integer newOffset = def.offset();
        if (def.limitParam() != null) {
            newLimit = resolveLimitOffset(def.limitParam(), params, "LIMIT");
        }
        if (def.offsetParam() != null) {
            newOffset = resolveLimitOffset(def.offsetParam(), params, "OFFSET");
        }

        // Re-substitute the parser-collected window function list so the entries match
        // (by record value equality) the WindowFnCalls embedded in the rebuilt expressions
        // and criteria. WindowStage looks them up on Row by record equality.
        List<Expression.WindowFnCall> newWindowCalls = def.windowFunctionCalls().stream()
                .map(w -> substituteWindowFn(w, params, settings))
                .toList();

        return new QueryDefinition(
                newSelects,
                def.rootPath(),
                def.fromSubQuery(),
                newWhere,
                newGroupBy,
                newHaving,
                newOrderBy,
                def.referencedFields(),
                def.distinct(),
                newLimit,
                newOffset,
                def.containsNonDeterministic(),
                def.rootAlias(),
                def.joins(),
                null,
                null,
                0,
                def.namedParameters(),
                def.subqueryPositionalOffset(),
                newWindowCalls);
    }

    /**
     * Substitutes {@code ParameterRef}s inside a single expression subtree.
     *
     * @param expr     the expression tree (never {@code null})
     * @param params   the bound parameter values
     * @param settings the settings (used to resolve the {@link ConditionHandlerRegistry}
     *                 when walking {@link Expression.SearchedCaseWhen} conditions)
     * @return the substituted expression tree; the same instance is returned when no
     * {@code ParameterRef} node is encountered (for {@code LiteralVal}, {@code ColumnRef},
     * {@code NowRef}, and {@code COUNT(*)})
     * @throws SQL4JsonBindException on bind failure
     */
    public static Expression substituteExpression(Expression expr, BoundParameters params,
                                                  Sql4jsonSettings settings) {
        return switch (expr) {
            case Expression.ParameterRef p -> new Expression.LiteralVal(resolveParam(p, params));
            case Expression.ScalarFnCall fn -> substituteScalarFn(fn, params, settings);
            case Expression.AggregateFnCall agg -> substituteAggregate(agg, params, settings);
            case Expression.WindowFnCall win -> substituteWindowFn(win, params, settings);
            case Expression.SimpleCaseWhen sc -> substituteSimpleCase(sc, params, settings);
            case Expression.SearchedCaseWhen sc -> substituteSearchedCase(sc, params, settings);
            case Expression.LiteralVal lv -> lv;
            case Expression.ColumnRef cr -> cr;
            case Expression.NowRef nr -> nr;
        };
    }

    // ── SELECT / criteria / condition-context helpers ────────────────────────

    private static SelectColumnDef substituteSelectColumn(SelectColumnDef sc, BoundParameters params,
                                                          Sql4jsonSettings settings) {
        if (sc.isAsterisk()) {
            return sc;
        }
        Expression newExpr = substituteExpression(sc.expression(), params, settings);
        return SelectColumnDef.of(newExpr, sc.alias());
    }

    private static CriteriaNode substituteCriteria(CriteriaNode node, BoundParameters params,
                                                   Sql4jsonSettings settings,
                                                   ConditionHandlerRegistry registry) {
        return switch (node) {
            case null -> null;
            case AndNode(CriteriaNode left, CriteriaNode right) -> new AndNode(
                    substituteCriteria(left, params, settings, registry),
                    substituteCriteria(right, params, settings, registry));
            case OrNode(CriteriaNode left, CriteriaNode right) -> new OrNode(
                    substituteCriteria(left, params, settings, registry),
                    substituteCriteria(right, params, settings, registry));
            case SingleConditionNode(ConditionContext cc, CriteriaNode ignored) -> {
                ConditionContext newCc = substituteConditionContext(cc, params, settings);
                yield new SingleConditionNode(newCc, registry.resolve(newCc));
            }
            // Unknown leaf type — shouldn't happen. Return as-is.
            default -> node;
        };
    }

    private static ConditionContext substituteConditionContext(
            ConditionContext cc, BoundParameters params, Sql4jsonSettings settings) {

        // Array-operator predicates have a different RHS shape (whole-array bind) and
        // need to bypass the scalar-resolution path used for COMPARISON / LIKE / etc.
        if (isArrayOperator(cc.type())) {
            return substituteArrayPredicate(cc, params, settings);
        }

        rejectCollectionBindForContains(cc, params);

        Expression newLhs = substituteOrNull(cc.lhsExpression(), params, settings);
        Expression newRhsExpr = substituteOrNull(cc.rhsExpression(), params, settings);
        SqlValue newTestValue = newRhsExpr instanceof Expression.LiteralVal(SqlValue v)
                ? v
                : cc.testValue();

        // After substitution the value-expression list is always resolved into a flat
        // List<SqlValue>, so the expression list is cleared — this also prevents any
        // double-substitution if substitute() is ever called twice on the same node.
        List<SqlValue> newValueList = cc.valueExpressions() != null
                ? expandInList(cc.valueExpressions(), params, settings, cc.lhsExpression())
                : cc.valueList();

        SubstitutedBound lower = substituteBound(cc.lowerBoundExpr(), cc.lowerBound(), params, settings);
        SubstitutedBound upper = substituteBound(cc.upperBoundExpr(), cc.upperBound(), params, settings);

        return new ConditionContext(
                cc.type(),
                newLhs,
                cc.operator(),
                newTestValue,
                newRhsExpr,
                newValueList,
                lower.literal(),
                upper.literal(),
                null,
                lower.expression(),
                upper.expression());
    }

    /**
     * CONTAINS — scalar RHS, but with a clearer error message when the caller accidentally
     * binds a Collection (a common mistake for users migrating from PostgreSQL's {@code @>}).
     * Without this guard the bind would fall through {@code resolveParam} and surface the
     * generic "scalar placeholder" message that doesn't mention CONTAINS, leaving the user
     * to guess which operator complained.
     */
    private static void rejectCollectionBindForContains(ConditionContext cc, BoundParameters params) {
        if (cc.type() != ConditionContext.ConditionType.CONTAINS
                || !(cc.rhsExpression() instanceof Expression.ParameterRef p)) {
            return;
        }
        Object raw = (p.name() != null) ? params.getByName(p.name()) : params.getByIndex(p.index());
        if (raw instanceof java.util.Collection<?> || (raw != null && raw.getClass().isArray())) {
            throw new SQL4JsonBindException(
                    "CONTAINS expects a scalar bind for parameter " + paramLabel(p)
                            + "; got Collection. Use @>, <@, &&, or = with a list/array bind"
                            + " for whole-array operators.");
        }
    }

    private static Expression substituteOrNull(Expression expr, BoundParameters params,
                                               Sql4jsonSettings settings) {
        return expr == null ? null : substituteExpression(expr, params, settings);
    }

    /**
     * Substitutes a BETWEEN bound expression. Returns the resolved literal in
     * {@link SubstitutedBound#literal()} when substitution reduces to a {@code LiteralVal};
     * otherwise the substituted expression survives in {@link SubstitutedBound#expression()}
     * for per-row evaluation by {@code BetweenConditionHandler}.
     */
    private static SubstitutedBound substituteBound(Expression boundExpr, SqlValue defaultLiteral,
                                                    BoundParameters params, Sql4jsonSettings settings) {
        if (boundExpr == null) {
            return new SubstitutedBound(defaultLiteral, null);
        }
        Expression s = substituteExpression(boundExpr, params, settings);
        if (s instanceof Expression.LiteralVal(SqlValue lv)) {
            return new SubstitutedBound(lv, null);
        }
        return new SubstitutedBound(defaultLiteral, s);
    }

    private record SubstitutedBound(SqlValue literal, Expression expression) {
        // Carrier for substituteBound() — either literal is non-null (resolved) or expression is
        // non-null (deferred to per-row evaluation); never both.
    }

    /**
     * Resolves a LIMIT / OFFSET parameter placeholder to an {@link Integer}.
     *
     * <p>Full validation matrix:
     * <ul>
     *   <li>{@code Byte}, {@code Short}, {@code Integer}, {@code Long} — accepted if within
     *       {@code [0, Integer.MAX_VALUE]}.</li>
     *   <li>{@code BigInteger} — accepted via {@code longValueExact()}; overflow throws.</li>
     *   <li>{@code BigDecimal} with scale &le; 0 after {@code stripTrailingZeros()} and within
     *       int range — accepted; non-zero fractional part throws.</li>
     *   <li>{@code Double}, {@code Float}, or any other {@code Number} subtype — rejected
     *       (non-integer numeric type).</li>
     *   <li>{@code null} or non-{@code Number} — rejected.</li>
     *   <li>Negative values or values {@code > Integer.MAX_VALUE} — rejected.</li>
     * </ul>
     *
     * @param ref    the placeholder reference (positional or named)
     * @param params the bound parameter carrier
     * @param clause {@code "LIMIT"} or {@code "OFFSET"}, used in error messages
     * @return the resolved non-negative integer value
     * @throws SQL4JsonBindException if the bound value fails any validation rule
     */
    private static Integer resolveLimitOffset(Expression.ParameterRef ref, BoundParameters params,
                                              String clause) {
        Object raw = (ref.name() != null) ? params.getByName(ref.name()) : params.getByIndex(ref.index());
        if (raw == null) {
            throw new SQL4JsonBindException(clause + " cannot be bound to null");
        }
        long v = coerceToIntegralLong(raw, clause);
        if (v < 0) {
            throw new SQL4JsonBindException(clause + " must be non-negative, got " + v);
        }
        if (v > Integer.MAX_VALUE) {
            throw new SQL4JsonBindException(clause + " exceeds Integer.MAX_VALUE: " + v);
        }
        return (int) v;
    }

    private static long coerceToIntegralLong(Object raw, String clause) {
        if (raw instanceof Byte || raw instanceof Short || raw instanceof Integer || raw instanceof Long) {
            return ((Number) raw).longValue();
        }
        if (raw instanceof BigInteger bi) {
            try {
                return bi.longValueExact();
            } catch (ArithmeticException e) {
                throw new SQL4JsonBindException(clause + " exceeds Integer.MAX_VALUE: " + bi, e);
            }
        }
        if (raw instanceof BigDecimal bd) {
            BigDecimal stripped = bd.stripTrailingZeros();
            if (stripped.scale() > 0) {
                throw new SQL4JsonBindException(clause + " must be an integer value, got " + bd);
            }
            try {
                return stripped.longValueExact();
            } catch (ArithmeticException e) {
                throw new SQL4JsonBindException(clause + " exceeds Integer.MAX_VALUE: " + bd, e);
            }
        }
        if (raw instanceof Number) {
            // Double, Float, AtomicInteger, or any other non-integer Number subtype
            throw new SQL4JsonBindException(
                    clause + " must be an integer, got " + raw.getClass().getSimpleName());
        }
        throw new SQL4JsonBindException(
                clause + " must be an integer, got " + raw.getClass().getName());
    }

    private static SqlValue resolveParam(Expression.ParameterRef p, BoundParameters params) {
        Object raw = (p.name() != null) ? params.getByName(p.name()) : params.getByIndex(p.index());
        if (raw instanceof java.util.Collection<?> || (raw != null && raw.getClass().isArray())) {
            throw new SQL4JsonBindException(rejectCollectionMessage(p, paramLabel(p)));
        }
        return ParameterConverter.toSqlValue(raw);
    }

    /**
     * Builds the printable label for a {@link Expression.ParameterRef} — {@code ":name"} for
     * named placeholders, {@code "position N"} for positional ones. Used in bind-time error
     * messages.
     */
    private static String paramLabel(Expression.ParameterRef p) {
        return p.name() != null ? ":" + p.name() : "position " + p.index();
    }

    /**
     * Builds a position-aware error message when a Collection / array is bound to a
     * scalar placeholder slot. {@link ParameterPositionKind#ARRAY_ELEMENT} ({@code ARRAY[?]})
     * gets a hint pointing at the bare-RHS form; everything else falls back to the
     * generic message. {@code BARE_ARRAY_RHS} and {@code IN_LIST} are not routed
     * through {@code resolveParam} (they go through {@code expandArrayParameter} /
     * {@code expandInListElement}); the branch exists only as a safety net so an
     * unexpected dispatch doesn't yield a misleading message.
     *
     * @param p     the placeholder reference (carries the position kind)
     * @param label printable label (e.g. {@code ":req"} or {@code "position 0"})
     * @return a human-readable error message
     */
    private static String rejectCollectionMessage(Expression.ParameterRef p, String label) {
        return switch (p.positionKind()) {
            case ARRAY_ELEMENT -> "Cannot bind Collection to ARRAY[?] slot at " + label
                    + "; ARRAY[?] requires a scalar bind. "
                    + "Use a bare ?/:name (whole-array RHS) instead.";
            case REGULAR_SCALAR, BARE_ARRAY_RHS, IN_LIST -> "Cannot bind collection to scalar placeholder at " + label;
        };
    }

    /**
     * Expands an IN / NOT IN value-expression list into a flat {@code List<SqlValue>}, honouring
     * collection / array expansion for {@code ParameterRef} elements.
     *
     * <p>For each value expression:
     * <ul>
     *   <li>{@code LiteralVal} — its {@code SqlValue} is appended.</li>
     *   <li>{@code ParameterRef} — the raw Java value is read; a {@link java.util.Collection} or
     *       array expands into one {@code SqlValue} per element, anything else becomes a single
     *       element. This is the <b>only</b> context in which collections are permitted.</li>
     *   <li>Any other expression — substituted recursively and must reduce to a literal.</li>
     * </ul>
     *
     * <p>The post-expansion size is checked against {@link
     * io.github.mnesimiyilmaz.sql4json.settings.LimitsSettings#maxInListSize()}. Exceeding it
     * throws {@link SQL4JsonBindException}.
     *
     * @param valueExprs    the condition's value-expression list (non-null)
     * @param params        the bound parameter values
     * @param settings      the settings supplying the limits
     * @param lhsForContext the LHS expression of the enclosing condition (for error messages;
     *                      may be {@code null})
     * @return an immutable flat list of resolved {@code SqlValue}s
     * @throws SQL4JsonBindException on bind failure or limit violation
     */
    private static List<SqlValue> expandInList(List<Expression> valueExprs,
                                               BoundParameters params,
                                               Sql4jsonSettings settings,
                                               Expression lhsForContext) {
        List<SqlValue> out = new ArrayList<>(valueExprs.size());
        for (Expression e : valueExprs) {
            expandInListElement(e, params, settings, out);
        }
        int max = settings.limits().maxInListSize();
        if (out.size() > max) {
            String fieldLabel = lhsForContext != null ? lhsForContext.innermostColumnPath() : "<?>";
            throw new SQL4JsonBindException(
                    "IN list size after expansion exceeds configured maximum ("
                            + max + ") at " + fieldLabel);
        }
        return List.copyOf(out);
    }

    private static void expandInListElement(Expression e, BoundParameters params,
                                            Sql4jsonSettings settings, List<SqlValue> out) {
        if (e instanceof Expression.LiteralVal(SqlValue v)) {
            out.add(v);
            return;
        }
        if (e instanceof Expression.ParameterRef(String name, int index, var positionKind)) {
            Object raw = (name != null) ? params.getByName(name) : params.getByIndex(index);
            addExpandedParameter(raw, out);
            return;
        }
        if (e instanceof Expression.NowRef()) {
            // Snapshot semantics for NOW() in a parameterized IN list: substitution captures
            // a single LocalDateTime here and embeds it as a literal, so every row in the
            // execution sees the *same* timestamp. This matches the JDBC-style "bind once,
            // execute" model and is intentionally distinct from the non-parameterized path,
            // where NowRef survives into InConditionHandler/BetweenConditionHandler and is
            // re-evaluated per row. Documented on BoundParameters; do not change without
            // updating that contract.
            out.add(new SqlDateTime(LocalDateTime.now()));
            return;
        }
        // Other expression types (shouldn't typically appear in IN list) — substitute
        // and extract literal.
        Expression s = substituteExpression(e, params, settings);
        if (!(s instanceof Expression.LiteralVal(SqlValue lv))) {
            throw new IllegalStateException("IN-list element did not reduce to literal: " + s);
        }
        out.add(lv);
    }

    private static boolean isArrayOperator(ConditionContext.ConditionType type) {
        return type == ConditionContext.ConditionType.ARRAY_CONTAINS
                || type == ConditionContext.ConditionType.ARRAY_CONTAINED_BY
                || type == ConditionContext.ConditionType.ARRAY_OVERLAP
                || type == ConditionContext.ConditionType.ARRAY_EQUALS
                || type == ConditionContext.ConditionType.ARRAY_NOT_EQUALS;
    }

    /**
     * Substitutes parameters inside an array-operator {@link ConditionContext}.
     *
     * <p>Three cases:
     * <ul>
     *   <li>{@code valueExpressions} is non-null (parsed {@code ARRAY[…]} literal) — each
     *       element expression is substituted recursively; the resulting list stays in
     *       {@code valueExpressions}.</li>
     *   <li>{@code rhsExpression} is a {@link Expression.ParameterRef} (bare {@code ?} /
     *       {@code :name}) — the bound value must be a {@link java.util.Collection} or
     *       array; each element is wrapped into a {@link Expression.LiteralVal} and the
     *       flat list lands in {@code valueExpressions}. The {@code rhsExpression} field
     *       is cleared.</li>
     *   <li>{@code rhsExpression} is anything else (column-ref) — substituted recursively
     *       and kept in {@code rhsExpression}.</li>
     * </ul>
     */
    private static ConditionContext substituteArrayPredicate(
            ConditionContext cc, BoundParameters params, Sql4jsonSettings settings) {

        Expression newLhs = cc.lhsExpression() == null
                ? null
                : substituteExpression(cc.lhsExpression(), params, settings);

        // Case A — array literal (or already-substituted form): substitute each element
        if (cc.valueExpressions() != null) {
            List<Expression> newElements = cc.valueExpressions().stream()
                    .map(e -> substituteExpression(e, params, settings))
                    .toList();
            return new ConditionContext(cc.type(), newLhs, cc.operator(), null, null,
                    null, null, null, newElements, null, null);
        }

        Expression rhs = cc.rhsExpression();
        if (rhs == null) {
            throw new SQL4JsonExecutionException(
                    "array predicate has no RHS expression — listener bug for type " + cc.type());
        }

        // Case B — bare parameter
        if (rhs instanceof Expression.ParameterRef p) {
            Object raw = (p.name() != null) ? params.getByName(p.name()) : params.getByIndex(p.index());
            List<Expression> elements = expandArrayParameter(raw, p, cc.operator());
            return new ConditionContext(cc.type(), newLhs, cc.operator(), null, null,
                    null, null, null, elements, null, null);
        }

        // Case C — column-ref (or other deferred expression) — substitute, keep on rhsExpression
        Expression newRhs = substituteExpression(rhs, params, settings);
        return new ConditionContext(cc.type(), newLhs, cc.operator(), null, newRhs,
                null, null, null, null, null, null);
    }

    /**
     * Expands a {@link java.util.Collection} or array bound to an array-operator parameter into
     * a list of {@link Expression.LiteralVal} entries (one per element, converted via
     * {@link ParameterConverter#toSqlValue(Object)}).
     *
     * @param raw      the bound parameter value
     * @param p        the placeholder reference (used for error messages)
     * @param operator the array operator symbol (used for error messages)
     * @return an immutable list of literal expressions, one per element
     * @throws SQL4JsonBindException if the bound value is null, not a collection, or not an array
     */
    private static List<Expression> expandArrayParameter(Object raw, Expression.ParameterRef p,
                                                         String operator) {
        String label = paramLabel(p);
        if (raw == null) {
            throw new SQL4JsonBindException(
                    "array operator '" + operator + "' parameter " + label
                            + " bound to null; expected a Collection");
        }
        if (raw instanceof java.util.Collection<?> coll) {
            List<Expression> out = new ArrayList<>(coll.size());
            for (Object el : coll) {
                out.add(new Expression.LiteralVal(ParameterConverter.toSqlValue(el)));
            }
            return List.copyOf(out);
        }
        if (raw.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(raw);
            List<Expression> out = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                out.add(new Expression.LiteralVal(
                        ParameterConverter.toSqlValue(java.lang.reflect.Array.get(raw, i))));
            }
            return List.copyOf(out);
        }
        throw new SQL4JsonBindException(
                "array operator '" + operator + "' parameter " + label
                        + " must bind to a Collection; got " + raw.getClass().getSimpleName());
    }

    private static void addExpandedParameter(Object raw, List<SqlValue> out) {
        if (raw instanceof java.util.Collection<?> coll) {
            for (Object el : coll) {
                out.add(ParameterConverter.toSqlValue(el));
            }
            return;
        }
        if (raw != null && raw.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(raw);
            for (int i = 0; i < len; i++) {
                out.add(ParameterConverter.toSqlValue(java.lang.reflect.Array.get(raw, i)));
            }
            return;
        }
        out.add(ParameterConverter.toSqlValue(raw));
    }

    private static Expression.ScalarFnCall substituteScalarFn(
            Expression.ScalarFnCall fn, BoundParameters params, Sql4jsonSettings settings) {
        List<Expression> newArgs = substituteAll(fn.args(), params, settings);
        return new Expression.ScalarFnCall(fn.name(), newArgs);
    }

    private static Expression.AggregateFnCall substituteAggregate(
            Expression.AggregateFnCall agg, BoundParameters params, Sql4jsonSettings settings) {
        if (agg.inner() == null) {
            return agg; // COUNT(*) — no inner expression
        }
        return new Expression.AggregateFnCall(
                agg.name(), substituteExpression(agg.inner(), params, settings));
    }

    private static Expression.WindowFnCall substituteWindowFn(
            Expression.WindowFnCall win, BoundParameters params, Sql4jsonSettings settings) {
        List<Expression> newArgs = substituteAll(win.args(), params, settings);
        WindowSpec newSpec = substituteWindowSpec(win.spec(), params, settings);
        return new Expression.WindowFnCall(win.name(), newArgs, newSpec);
    }

    private static WindowSpec substituteWindowSpec(WindowSpec spec, BoundParameters params,
                                                   Sql4jsonSettings settings) {
        List<Expression> newPartition = substituteAll(spec.partitionBy(), params, settings);
        List<OrderByColumnDef> newOrder = spec.orderBy().stream()
                .map(o -> OrderByColumnDef.of(
                        substituteExpression(o.expression(), params, settings), o.direction()))
                .toList();
        return new WindowSpec(newPartition, newOrder);
    }

    private static Expression.SimpleCaseWhen substituteSimpleCase(
            Expression.SimpleCaseWhen sc, BoundParameters params, Sql4jsonSettings settings) {
        Expression newSubject = substituteExpression(sc.subject(), params, settings);
        List<Expression.WhenClause.ValueWhen> newClauses = new ArrayList<>(sc.whenClauses().size());
        for (Expression.WhenClause.ValueWhen wc : sc.whenClauses()) {
            newClauses.add(new Expression.WhenClause.ValueWhen(
                    substituteExpression(wc.value(), params, settings),
                    substituteExpression(wc.result(), params, settings)));
        }
        Expression newElse = sc.elseExpr() == null
                ? null
                : substituteExpression(sc.elseExpr(), params, settings);
        return new Expression.SimpleCaseWhen(newSubject, newClauses, newElse);
    }

    private static Expression.SearchedCaseWhen substituteSearchedCase(
            Expression.SearchedCaseWhen sc, BoundParameters params, Sql4jsonSettings settings) {
        ConditionHandlerRegistry registry = ConditionHandlerRegistry.forSettings(settings);
        List<Expression.WhenClause.SearchWhen> newClauses = new ArrayList<>(sc.whenClauses().size());
        for (Expression.WhenClause.SearchWhen wc : sc.whenClauses()) {
            newClauses.add(new Expression.WhenClause.SearchWhen(
                    substituteCriteria(wc.condition(), params, settings, registry),
                    wc.conditionFields(),
                    substituteExpression(wc.result(), params, settings)));
        }
        Expression newElse = sc.elseExpr() == null
                ? null
                : substituteExpression(sc.elseExpr(), params, settings);
        return new Expression.SearchedCaseWhen(newClauses, newElse);
    }

    private static List<Expression> substituteAll(List<Expression> list, BoundParameters params,
                                                  Sql4jsonSettings settings) {
        List<Expression> out = new ArrayList<>(list.size());
        for (Expression e : list) {
            out.add(substituteExpression(e, params, settings));
        }
        return out;
    }
}
