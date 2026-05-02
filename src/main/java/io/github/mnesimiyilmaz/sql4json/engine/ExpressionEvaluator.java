// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.engine.Expression.*;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;
import io.github.mnesimiyilmaz.sql4json.types.SqlDateTime;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Tree-walking interpreter for {@link Expression} trees.
 *
 * <p>Two evaluation modes:
 *
 * <ul>
 *   <li>{@link #evaluate} — per-row evaluation of scalar expressions
 *   <li>{@link #evaluateAggregate} — group evaluation: splits at AggregateFnCall, evaluates inner per-row, aggregates,
 *       then applies outer scalars
 * </ul>
 */
public final class ExpressionEvaluator {

    private ExpressionEvaluator() {}

    /**
     * Evaluate a scalar expression against a single row. Throws if an AggregateFnCall is encountered (use
     * evaluateAggregate instead).
     *
     * @param expr the expression tree to evaluate
     * @param row the data row to evaluate against
     * @param registry function registry for scalar function lookup
     * @return the computed SQL value
     */
    public static SqlValue evaluate(Expression expr, RowAccessor row, FunctionRegistry registry) {
        return switch (expr) {
            case ColumnRef(var path) -> row.get(FieldKey.of(path));
            case LiteralVal(var value) -> value;
            case ScalarFnCall(var name, var args) -> {
                // Evaluate first arg as primary value, rest as extra args
                if (args.isEmpty()) {
                    throw new SQL4JsonExecutionException(
                            "Scalar function '" + name + "' requires at least one argument");
                }
                SqlValue primary = evaluate(args.getFirst(), row, registry);
                List<SqlValue> extraArgs = new ArrayList<>(args.size() - 1);
                for (int i = 1; i < args.size(); i++) {
                    extraArgs.add(evaluate(args.get(i), row, registry));
                }
                yield registry.getScalar(name)
                        .orElseThrow(() -> new SQL4JsonExecutionException("Unknown scalar function: " + name))
                        .apply()
                        .apply(primary, extraArgs);
            }
            case AggregateFnCall ignored ->
                throw new SQL4JsonExecutionException(
                        "Aggregate function cannot be evaluated per-row. Use evaluateAggregate().");
            case Expression.WindowFnCall wfc -> resolveWindowResult(wfc, row);
            case SimpleCaseWhen(var subject, var clauses, var elseExpr) -> {
                SqlValue subjectVal = evaluate(subject, row, registry);
                for (var wc : clauses) {
                    SqlValue whenVal = evaluate(wc.value(), row, registry);
                    if (!subjectVal.isNull()
                            && !whenVal.isNull()
                            && SqlValueComparator.compare(subjectVal, whenVal) == 0) {
                        yield evaluate(wc.result(), row, registry);
                    }
                }
                yield elseExpr != null ? evaluate(elseExpr, row, registry) : SqlNull.INSTANCE;
            }
            case SearchedCaseWhen(var clauses, var elseExpr) -> {
                for (var wc : clauses) {
                    if (wc.condition().test(row)) {
                        yield evaluate(wc.result(), row, registry);
                    }
                }
                yield elseExpr != null ? evaluate(elseExpr, row, registry) : SqlNull.INSTANCE;
            }
            case NowRef() -> new SqlDateTime(LocalDateTime.now());
            case Expression.ParameterRef ignored ->
                throw new IllegalStateException("ParameterRef reached evaluator — parameter substitution was skipped");
        };
    }

    /**
     * Evaluate an expression that may contain an aggregate, against a group of rows. Splits at the AggregateFnCall:
     * evaluates inner per-row, aggregates, then applies any outer scalar functions to the result.
     *
     * @param expr the expression tree (may contain aggregate calls)
     * @param group the group of rows to aggregate over
     * @param registry function registry for function lookup
     * @return the computed aggregate SQL value
     */
    public static SqlValue evaluateAggregate(
            Expression expr, List<? extends RowAccessor> group, FunctionRegistry registry) {
        return switch (expr) {
            case AggregateFnCall(var name, var inner) -> {
                List<SqlValue> values;
                if (inner == null) {
                    // COUNT(*): one placeholder per row — aggregate counts the list size
                    values = group.stream().<SqlValue>map(r -> SqlNull.INSTANCE).toList();
                } else {
                    values = group.stream()
                            .map(row -> evaluate(inner, row, registry))
                            .toList();
                    // For COUNT(col), filter out NULLs per SQL standard
                    if ("count".equalsIgnoreCase(name)) {
                        values = values.stream().filter(v -> !v.isNull()).toList();
                    }
                }
                yield registry.getAggregate(name)
                        .orElseThrow(() -> new SQL4JsonExecutionException("Unknown aggregate function: " + name))
                        .apply()
                        .apply(values);
            }
            case ScalarFnCall(var name, var args) -> {
                List<SqlValue> evaluatedArgs = new ArrayList<>(args.size());
                for (Expression arg : args) {
                    evaluatedArgs.add(evalExpr(arg, group, registry));
                }
                SqlValue primary = evaluatedArgs.getFirst();
                List<SqlValue> extraArgs = evaluatedArgs.subList(1, evaluatedArgs.size());
                yield registry.getScalar(name)
                        .orElseThrow(() -> new SQL4JsonExecutionException("Unknown scalar function: " + name))
                        .apply()
                        .apply(primary, extraArgs);
            }
            case ColumnRef ignored -> evaluate(expr, group.getFirst(), registry);
            case LiteralVal ignored -> evaluate(expr, group.getFirst(), registry);
            case Expression.WindowFnCall wfc -> resolveWindowResult(wfc, group.getFirst());
            case SimpleCaseWhen(var subject, var clauses, var elseExpr) ->
                aggregateSimpleCase(subject, clauses, elseExpr, group, registry);
            case SearchedCaseWhen(var clauses, var elseExpr) ->
                aggregateSearchedCase(clauses, elseExpr, group, registry);
            case NowRef() -> new SqlDateTime(LocalDateTime.now());
            case Expression.ParameterRef ignored ->
                throw new IllegalStateException("ParameterRef reached evaluator — parameter substitution was skipped");
        };
    }

    /**
     * Evaluate an expression in aggregate context: if it contains an aggregate, delegate to evaluateAggregate;
     * otherwise evaluate against the first row.
     */
    private static SqlValue evalExpr(Expression expr, List<? extends RowAccessor> group, FunctionRegistry registry) {
        return expr.containsAggregate()
                ? evaluateAggregate(expr, group, registry)
                : evaluate(expr, group.getFirst(), registry);
    }

    private static SqlValue resolveWindowResult(Expression.WindowFnCall wfc, RowAccessor row) {
        SqlValue v = row.getWindowResult(wfc);
        if (v == null) {
            throw new SQL4JsonExecutionException("Window function '" + wfc.name()
                    + "' cannot be evaluated here — window functions are only valid in"
                    + " SELECT (and CASE expressions inside SELECT). They cannot appear"
                    + " in WHERE, HAVING, GROUP BY, or JOIN ON.");
        }
        return v;
    }

    private static SqlValue aggregateSimpleCase(
            Expression subject,
            List<Expression.WhenClause.ValueWhen> clauses,
            Expression elseExpr,
            List<? extends RowAccessor> group,
            FunctionRegistry registry) {
        SqlValue subjectVal = evalExpr(subject, group, registry);
        for (var wc : clauses) {
            SqlValue whenVal = evalExpr(wc.value(), group, registry);
            if (!subjectVal.isNull() && !whenVal.isNull() && SqlValueComparator.compare(subjectVal, whenVal) == 0) {
                return evalExpr(wc.result(), group, registry);
            }
        }
        return elseExpr != null ? evalExpr(elseExpr, group, registry) : SqlNull.INSTANCE;
    }

    private static SqlValue aggregateSearchedCase(
            List<Expression.WhenClause.SearchWhen> clauses,
            Expression elseExpr,
            List<? extends RowAccessor> group,
            FunctionRegistry registry) {
        for (var wc : clauses) {
            if (wc.condition().test(group.getFirst())) {
                return evalExpr(wc.result(), group, registry);
            }
        }
        return elseExpr != null ? evalExpr(elseExpr, group, registry) : SqlNull.INSTANCE;
    }
}
