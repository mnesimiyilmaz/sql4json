package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.List;

/**
 * Immutable context carrying all parameters needed to build a {@link CriteriaNode}
 * from a parsed WHERE/HAVING condition. Passed to {@link ConditionHandler} implementations.
 *
 * @param type          the type of condition being evaluated
 * @param lhsExpression the left-hand side expression tree
 * @param operator      the comparison operator string (e.g. {@code "="}, {@code "!="})
 * @param testValue     the literal value to compare against (for simple comparisons)
 * @param rhsExpression the right-hand side expression tree (for column-to-column or dynamic LIKE)
 * @param valueList     the list of values for IN / NOT IN conditions
 * @param lowerBound    the lower bound for BETWEEN conditions
 * @param upperBound    the upper bound for BETWEEN conditions
 */
public record ConditionContext(
        ConditionType type,
        Expression lhsExpression,     // Left-hand side expression tree
        String operator,
        SqlValue testValue,
        Expression rhsExpression,     // Right-hand side expression tree (for col-to-col, dynamic LIKE)
        List<SqlValue> valueList,     // IN / NOT IN value list
        SqlValue lowerBound,          // BETWEEN lower bound
        SqlValue upperBound           // BETWEEN upper bound
) {
    /**
     * Backward compat: innermost column path.
     *
     * @return the innermost column path from the left-hand expression, or {@code null}
     */
    public String columnName() {
        return lhsExpression != null ? lhsExpression.innermostColumnPath() : null;
    }

    /**
     * The type of condition being evaluated.
     */
    public enum ConditionType {
        /**
         * A standard comparison condition (e.g. {@code =}, {@code >}).
         */
        COMPARISON,
        /**
         * A LIKE pattern-matching condition.
         */
        LIKE,
        /**
         * An IS NULL condition.
         */
        IS_NULL,
        /**
         * An IS NOT NULL condition.
         */
        IS_NOT_NULL,
        /**
         * An IN list condition.
         */
        IN,
        /**
         * A NOT IN list condition.
         */
        NOT_IN,
        /**
         * A BETWEEN range condition.
         */
        BETWEEN,
        /**
         * A NOT BETWEEN range condition.
         */
        NOT_BETWEEN,
        /**
         * A NOT LIKE pattern-matching condition.
         */
        NOT_LIKE
    }
}
