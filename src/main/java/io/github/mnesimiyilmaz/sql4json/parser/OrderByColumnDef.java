package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.ColumnRef;

/**
 * Defines a single column in an ORDER BY clause, combining an expression with a sort direction.
 *
 * @param expression the expression to evaluate for sorting
 * @param direction  the sort direction ({@code "ASC"} or {@code "DESC"})
 */
public record OrderByColumnDef(Expression expression, String direction) {

    /**
     * Returns the innermost column path from the expression, or {@code null}.
     *
     * @return the column name, or {@code null} if not a simple column reference
     */
    public String columnName() {
        return expression != null ? expression.innermostColumnPath() : null;
    }

    /**
     * Creates an ascending ORDER BY definition for the given column name.
     *
     * @param columnName the column name to sort by
     * @return a new OrderByColumnDef with ASC direction
     */
    public static OrderByColumnDef of(String columnName) {
        return new OrderByColumnDef(new ColumnRef(columnName), "ASC");
    }

    /**
     * Creates an ORDER BY definition for the given column name and direction.
     *
     * @param columnName the column name to sort by
     * @param direction  the sort direction ({@code "ASC"} or {@code "DESC"})
     * @return a new OrderByColumnDef
     */
    public static OrderByColumnDef of(String columnName, String direction) {
        return new OrderByColumnDef(new ColumnRef(columnName), direction.toUpperCase());
    }

    /**
     * Creates an ORDER BY definition for the given expression and direction.
     *
     * @param expression the expression to sort by
     * @param direction  the sort direction ({@code "ASC"} or {@code "DESC"})
     * @return a new OrderByColumnDef
     */
    public static OrderByColumnDef of(Expression expression, String direction) {
        return new OrderByColumnDef(expression, direction.toUpperCase());
    }
}
