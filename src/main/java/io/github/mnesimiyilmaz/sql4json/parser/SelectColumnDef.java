package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.*;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.List;

/**
 * Represents a single column in a SELECT clause.
 * Wraps an {@link Expression} tree with an optional alias.
 *
 * @param expression the expression tree for this column (null for asterisk)
 * @param alias      optional column alias from AS clause
 * @param isAsterisk true if this is a {@code SELECT *} column
 */
public record SelectColumnDef(
        Expression expression,
        String alias,
        boolean isAsterisk
) {
    /**
     * Returns alias if set, otherwise the innermost column path from the expression,
     * or the expression toString as fallback.
     *
     * @return the effective output name for this column
     */
    public String aliasOrName() {
        if (alias != null) return alias;
        String col = columnName();
        return col != null ? col : expression.toString();
    }

    /**
     * Returns the innermost column path referenced by this expression.
     * Used by SelectStage for field projection and by JsonUnflattener for field navigation.
     *
     * @return the column path, or {@code null} for asterisk or non-column expressions
     */
    public String columnName() {
        return expression != null ? expression.innermostColumnPath() : null;
    }

    /**
     * Returns true if this expression contains an aggregate function.
     * Used by SelectStage, JsonUnflattener, GroupAggregator to distinguish
     * aggregate columns from plain/scalar columns.
     *
     * @return {@code true} if an aggregate function is present in the expression tree
     */
    public boolean containsAggregate() {
        return expression != null && expression.containsAggregate();
    }

    /**
     * Returns true if this expression contains a window function.
     * Window functions are NOT aggregates — they don't collapse rows.
     *
     * @return {@code true} if a window function is present in the expression tree
     */
    public boolean containsWindow() {
        return expression != null && expression.containsWindow();
    }

    /**
     * Returns the aggregate function name if the top-level or any nested expression
     * contains one, or null. Searches the full tree depth.
     *
     * @return the aggregate function name (e.g. {@code "SUM"}), or {@code null} if none
     */
    public String aggFunction() {
        return expression != null ? findAggregate(expression) : null;
    }

    private static String findAggregate(Expression expr) {
        return switch (expr) {
            case AggregateFnCall(var name, var inner) -> name;
            case ScalarFnCall(var name, var args) -> {
                for (Expression arg : args) {
                    String found = findAggregate(arg);
                    if (found != null) yield found;
                }
                yield null;
            }
            case SimpleCaseWhen(var subject, var clauses, var elseExpr) -> {
                String found = findAggregate(subject);
                if (found != null) yield found;
                for (var wc : clauses) {
                    found = findAggregate(wc.value());
                    if (found != null) yield found;
                    found = findAggregate(wc.result());
                    if (found != null) yield found;
                }
                yield elseExpr != null ? findAggregate(elseExpr) : null;
            }
            case SearchedCaseWhen(var clauses, var elseExpr) -> {
                for (var wc : clauses) {
                    String found = findAggregate(wc.result());
                    if (found != null) yield found;
                }
                yield elseExpr != null ? findAggregate(elseExpr) : null;
            }
            case Expression.ColumnRef ignored -> null;
            case Expression.LiteralVal ignored -> null;
            case Expression.WindowFnCall ignored -> null;
            case Expression.NowRef() -> null;
        };
    }

    // ── Factory methods ──────────────────────────────────────────────────

    /**
     * Creates a {@code SELECT *} column definition.
     *
     * @return a new asterisk SelectColumnDef
     */
    public static SelectColumnDef asterisk() {
        return new SelectColumnDef(null, null, true);
    }

    /**
     * Creates a SelectColumnDef from an expression with an optional alias.
     *
     * @param expression the expression tree for this column
     * @param alias      the alias, or {@code null}
     * @return a new SelectColumnDef
     */
    public static SelectColumnDef of(Expression expression, String alias) {
        return new SelectColumnDef(expression, alias, false);
    }

    /**
     * Backward compat: plain column.
     *
     * @param name the column name
     * @return a new SelectColumnDef for a plain column reference
     */
    public static SelectColumnDef column(String name) {
        return new SelectColumnDef(new ColumnRef(name), null, false);
    }

    /**
     * Backward compat: plain column with alias.
     *
     * @param name  the column name
     * @param alias the column alias
     * @return a new SelectColumnDef for a plain column reference with alias
     */
    public static SelectColumnDef column(String name, String alias) {
        return new SelectColumnDef(new ColumnRef(name), alias, false);
    }

    /**
     * Backward compat: aggregate.
     *
     * @param aggFn   the aggregate function name (e.g. {@code "SUM"})
     * @param colName the column name, or {@code "*"} for COUNT(*)
     * @param alias   the column alias
     * @return a new SelectColumnDef wrapping an aggregate function call
     */
    public static SelectColumnDef aggregate(String aggFn, String colName, String alias) {
        Expression inner = "*".equals(colName) ? null : new ColumnRef(colName);
        return new SelectColumnDef(new AggregateFnCall(aggFn.toUpperCase(), inner), alias, false);
    }

    /**
     * Backward compat: scalar function.
     *
     * @param fnName  the scalar function name
     * @param colName the column name passed as the first argument
     * @param alias   the column alias
     * @param fnArgs  additional literal arguments, or {@code null}
     * @return a new SelectColumnDef wrapping a scalar function call
     */
    public static SelectColumnDef scalar(String fnName, String colName, String alias,
                                         List<SqlValue> fnArgs) {
        var args = new java.util.ArrayList<Expression>();
        args.add(new ColumnRef(colName));
        if (fnArgs != null) {
            fnArgs.forEach(v -> args.add(new LiteralVal(v)));
        }
        return new SelectColumnDef(
                new ScalarFnCall(fnName.toLowerCase(), args), alias, false);
    }
}
