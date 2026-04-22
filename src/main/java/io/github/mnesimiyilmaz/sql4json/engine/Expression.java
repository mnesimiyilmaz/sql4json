package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.registry.CriteriaNode;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.List;
import java.util.Set;

/**
 * AST node for SQL column expressions. Used everywhere: SELECT, WHERE, HAVING,
 * GROUP BY, ORDER BY. Evaluated recursively by {@link ExpressionEvaluator}.
 *
 * <p>Follows the standard SQL engine pattern: everything that produces a value
 * is an Expression. Function arguments are Expressions, enabling arbitrary nesting
 * like {@code ROUND(AVG(NULLIF(salary, 0)), 2)}.
 */
public sealed interface Expression
        permits Expression.ColumnRef, Expression.ScalarFnCall, Expression.AggregateFnCall,
        Expression.LiteralVal, Expression.WindowFnCall,
        Expression.SimpleCaseWhen, Expression.SearchedCaseWhen, Expression.NowRef,
        Expression.ParameterRef {

    /**
     * Column/field reference: {@code name}, {@code address.city}
     *
     * @param path dot-separated path to the JSON field
     */
    record ColumnRef(String path) implements Expression {
    }

    /**
     * Scalar function call: {@code UPPER(name)}, {@code NULLIF(col, 0)}.
     * Arguments are Expressions — enabling nesting: {@code TRIM(NULLIF(col, ''))}.
     *
     * @param name function name (case-insensitive)
     * @param args function arguments as expression trees
     */
    record ScalarFnCall(String name, List<Expression> args) implements Expression {
        /**
         * Creates a defensive copy of the argument list.
         */
        public ScalarFnCall {
            args = List.copyOf(args);
        }
    }

    /**
     * Aggregate function call: {@code COUNT(*)}, {@code AVG(NULLIF(col, 0))}.
     * Inner expression is null for {@code COUNT(*)}.
     *
     * @param name  aggregate function name (COUNT, SUM, AVG, MIN, MAX)
     * @param inner inner expression, or {@code null} for {@code COUNT(*)}
     */
    record AggregateFnCall(String name, Expression inner) implements Expression {
    }

    /**
     * Literal value: {@code 42}, {@code 'hello'}, {@code true}, {@code NULL}.
     *
     * @param value the literal SQL value
     */
    record LiteralVal(SqlValue value) implements Expression {
    }

    /**
     * Window function call: function + arguments + OVER specification.
     * Evaluated by WindowStage, not by ExpressionEvaluator.
     *
     * @param name window function name (ROW_NUMBER, RANK, DENSE_RANK, etc.)
     * @param args function arguments as expression trees
     * @param spec OVER clause specification (PARTITION BY / ORDER BY)
     */
    record WindowFnCall(
            String name,
            List<Expression> args,
            WindowSpec spec
    ) implements Expression {
        /**
         * Creates a defensive copy of the argument list.
         */
        public WindowFnCall {
            args = List.copyOf(args);
        }
    }

    /**
     * A single WHEN clause inside a CASE expression.
     */
    sealed interface WhenClause {
        /**
         * Returns the result expression for this WHEN branch.
         *
         * @return the result expression
         */
        Expression result();

        /**
         * Simple CASE WHEN: matches by value equality.
         *
         * @param value  the value to compare against the CASE subject
         * @param result the expression to evaluate when matched
         */
        record ValueWhen(Expression value, Expression result) implements WhenClause {
        }

        /**
         * Searched CASE WHEN: matches by boolean condition.
         *
         * @param condition       the condition to evaluate
         * @param conditionFields column paths referenced by the condition
         * @param result          the expression to evaluate when the condition is true
         */
        record SearchWhen(CriteriaNode condition, Set<String> conditionFields,
                          Expression result) implements WhenClause {
        }
    }

    /**
     * Simple CASE expression: {@code CASE subject WHEN val THEN result ... END}.
     *
     * @param subject     the expression being compared
     * @param whenClauses ordered list of WHEN value/result pairs
     * @param elseExpr    ELSE expression, or {@code null} if omitted
     */
    record SimpleCaseWhen(
            Expression subject,
            List<WhenClause.ValueWhen> whenClauses,
            Expression elseExpr
    ) implements Expression {
        /**
         * Creates a defensive copy of the WHEN clause list.
         */
        public SimpleCaseWhen {
            whenClauses = List.copyOf(whenClauses);
        }
    }

    /**
     * Searched CASE expression: {@code CASE WHEN condition THEN result ... END}.
     *
     * @param whenClauses ordered list of WHEN condition/result pairs
     * @param elseExpr    ELSE expression, or {@code null} if omitted
     */
    record SearchedCaseWhen(
            List<WhenClause.SearchWhen> whenClauses,
            Expression elseExpr
    ) implements Expression {
        /**
         * Creates a defensive copy of the WHEN clause list.
         */
        public SearchedCaseWhen {
            whenClauses = List.copyOf(whenClauses);
        }
    }

    /**
     * Reference to the current timestamp ({@code NOW()}).
     */
    record NowRef() implements Expression {
    }

    /**
     * Unresolved parameter placeholder. Substituted with a {@link LiteralVal} at execute
     * time by {@code ParameterSubstitutor}. Never seen by {@code ExpressionEvaluator}.
     *
     * @param name  named-parameter name, or {@code null} for positional
     * @param index positional index (0-based), or {@code -1} for named
     */
    record ParameterRef(String name, int index) implements Expression {
    }

    // ── Utility methods ───────────────────────────────────────────────────

    /**
     * Collects all column paths referenced by this expression tree.
     *
     * @param fields mutable set to collect field paths into
     */
    default void collectReferencedFields(Set<String> fields) {
        switch (this) {
            case ColumnRef(var path) -> fields.add(path);
            case ScalarFnCall(var name, var args) -> args.forEach(a -> a.collectReferencedFields(fields));
            case AggregateFnCall(var name, var inner) when inner != null -> inner.collectReferencedFields(fields);
            case AggregateFnCall ignored -> { /* COUNT(*) — no inner expression */ }
            case LiteralVal ignored -> { /* no fields to collect */ }
            case WindowFnCall(var name, var args, var spec) -> {
                args.forEach(a -> a.collectReferencedFields(fields));
                spec.partitionBy().forEach(e -> e.collectReferencedFields(fields));
                spec.orderBy().forEach(o -> o.expression().collectReferencedFields(fields));
            }
            case SimpleCaseWhen(var subject, var clauses, var elseExpr) -> {
                subject.collectReferencedFields(fields);
                for (var wc : clauses) {
                    wc.value().collectReferencedFields(fields);
                    wc.result().collectReferencedFields(fields);
                }
                if (elseExpr != null) elseExpr.collectReferencedFields(fields);
            }
            case SearchedCaseWhen(var clauses, var elseExpr) -> {
                for (var wc : clauses) {
                    fields.addAll(wc.conditionFields());
                    wc.result().collectReferencedFields(fields);
                }
                if (elseExpr != null) elseExpr.collectReferencedFields(fields);
            }
            case NowRef() -> { /* no fields */ }
            case ParameterRef ignored -> { /* no fields — unresolved placeholder */ }
        }
    }

    /**
     * Returns the innermost column path, or null if none exists.
     * Used for backward compatibility (e.g., SelectStage field projection).
     *
     * @return the innermost column path, or {@code null}
     */
    default String innermostColumnPath() {
        return switch (this) {
            case ColumnRef(var path) -> path;
            case ScalarFnCall(var name, var args) -> args.isEmpty() ? null : args.getFirst().innermostColumnPath();
            case AggregateFnCall(var name, var inner) -> inner != null ? inner.innermostColumnPath() : null;
            case LiteralVal ignored -> null;
            case WindowFnCall ignored -> null;
            case SimpleCaseWhen ignored -> null;
            case SearchedCaseWhen ignored -> null;
            case NowRef() -> null;
            case ParameterRef ignored -> null;
        };
    }

    /**
     * Returns true if this expression tree contains an AggregateFnCall.
     *
     * @return {@code true} if any descendant is an {@link AggregateFnCall}
     */
    default boolean containsAggregate() {
        return switch (this) {
            case AggregateFnCall ignored -> true;
            case ScalarFnCall(var name, var args) -> args.stream().anyMatch(Expression::containsAggregate);
            case ColumnRef ignored -> false;
            case LiteralVal ignored -> false;
            case WindowFnCall ignored -> false;
            case SimpleCaseWhen(var subject, var clauses, var elseExpr) -> subject.containsAggregate()
                    || clauses.stream().anyMatch(wc -> wc.value().containsAggregate() || wc.result().containsAggregate())
                    || (elseExpr != null && elseExpr.containsAggregate());
            case SearchedCaseWhen(var clauses, var elseExpr) ->
                    clauses.stream().anyMatch(wc -> wc.result().containsAggregate())
                            || (elseExpr != null && elseExpr.containsAggregate());
            case NowRef() -> false;
            case ParameterRef ignored -> false;
        };
    }

    /**
     * Returns true if this expression tree contains a WindowFnCall.
     * Note: window functions nested inside aggregates is not legal SQL, so
     * AggregateFnCall returns false unconditionally.
     *
     * @return {@code true} if any descendant is a {@link WindowFnCall}
     */
    default boolean containsWindow() {
        return switch (this) {
            case WindowFnCall ignored -> true;
            case ScalarFnCall(var name, var args) -> args.stream().anyMatch(Expression::containsWindow);
            case AggregateFnCall ignored -> false;
            case ColumnRef ignored -> false;
            case LiteralVal ignored -> false;
            case SimpleCaseWhen(var subject, var clauses, var elseExpr) -> subject.containsWindow()
                    || clauses.stream().anyMatch(wc -> wc.value().containsWindow() || wc.result().containsWindow())
                    || (elseExpr != null && elseExpr.containsWindow());
            case SearchedCaseWhen(var clauses, var elseExpr) ->
                    clauses.stream().anyMatch(wc -> wc.result().containsWindow())
                            || (elseExpr != null && elseExpr.containsWindow());
            case NowRef() -> false;
            case ParameterRef ignored -> false;
        };
    }
}
