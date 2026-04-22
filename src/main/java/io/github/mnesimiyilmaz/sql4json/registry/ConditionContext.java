package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.List;

/**
 * Immutable context carrying all parameters needed to build a {@link CriteriaNode}
 * from a parsed WHERE/HAVING condition. Passed to {@link ConditionHandler} implementations.
 *
 * <p>The {@code *Expression} fields exist to defer materialisation when a right-hand side
 * contains a parameter placeholder — parameter substitution (Tasks 11-14) resolves them
 * into the classic literal/list/bound fields before the condition handler runs, so
 * handlers never see {@link Expression.ParameterRef} directly.</p>
 *
 * @param type             the type of condition being evaluated
 * @param lhsExpression    the left-hand side expression tree
 * @param operator         the comparison operator string (e.g. {@code "="}, {@code "!="})
 * @param testValue        the literal value to compare against (for simple comparisons);
 *                         {@code null} when the RHS is a non-literal (column ref or parameter)
 * @param rhsExpression    the right-hand side expression tree (column-to-column, dynamic LIKE,
 *                         or parameter placeholder for COMPARISON / LIKE / NOT_LIKE)
 * @param valueList        the list of literal values for IN / NOT IN conditions; {@code null}
 *                         when any element is a parameter placeholder — see {@code valueExpressions}
 * @param lowerBound       the literal lower bound for BETWEEN conditions; {@code null} when
 *                         the lower bound is a parameter — see {@code lowerBoundExpr}
 * @param upperBound       the literal upper bound for BETWEEN conditions; {@code null} when
 *                         the upper bound is a parameter — see {@code upperBoundExpr}
 * @param valueExpressions the parallel expression list for IN / NOT IN when at least one
 *                         element is a parameter placeholder; {@code null} for all-literal lists
 * @param lowerBoundExpr   the lower-bound expression for BETWEEN when the bound is a parameter;
 *                         {@code null} when the bound is a literal (captured in {@code lowerBound})
 * @param upperBoundExpr   the upper-bound expression for BETWEEN when the bound is a parameter;
 *                         {@code null} when the bound is a literal (captured in {@code upperBound})
 */
public record ConditionContext(
        ConditionType type,
        Expression lhsExpression,     // Left-hand side expression tree
        String operator,
        SqlValue testValue,
        Expression rhsExpression,     // Right-hand side expression tree (for col-to-col, dynamic LIKE)
        List<SqlValue> valueList,     // IN / NOT IN value list (literal-only)
        SqlValue lowerBound,          // BETWEEN lower bound (literal)
        SqlValue upperBound,          // BETWEEN upper bound (literal)
        List<Expression> valueExpressions,  // IN / NOT IN when any element is a ParameterRef
        Expression lowerBoundExpr,          // BETWEEN lower bound when bound is a ParameterRef
        Expression upperBoundExpr           // BETWEEN upper bound when bound is a ParameterRef
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
