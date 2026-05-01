package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.List;

/**
 * Immutable context carrying all parameters needed to build a {@link CriteriaNode}
 * from a parsed WHERE/HAVING condition. Passed to {@link ConditionHandler} implementations.
 *
 * <p>The {@code *Expression} fields exist to defer materialisation when a right-hand side
 * contains a non-literal expression — either a parameter placeholder (e.g.
 * {@link Expression.ParameterRef}) or a dynamic value function such as
 * {@link Expression.NowRef}. Handlers that receive these fields are responsible for
 * evaluating them per-row via {@code ExpressionEvaluator}; handlers do see non-literal
 * expressions (including {@link Expression.NowRef}) directly rather than having them
 * pre-resolved.</p>
 *
 * @param type             the type of condition being evaluated
 * @param lhsExpression    the left-hand side expression tree
 * @param operator         the comparison operator string (e.g. {@code "="}, {@code "!="})
 * @param testValue        the literal value to compare against (for simple comparisons);
 *                         {@code null} when the RHS is a non-literal (column ref, parameter,
 *                         or dynamic value function)
 * @param rhsExpression    the right-hand side expression tree (column-to-column, dynamic LIKE,
 *                         parameter placeholder, or dynamic value function such as
 *                         {@link Expression.NowRef} for COMPARISON / LIKE / NOT_LIKE)
 * @param valueList        the list of literal values for IN / NOT IN conditions; {@code null}
 *                         when any element is a non-literal expression — see {@code valueExpressions}
 * @param lowerBound       the literal lower bound for BETWEEN conditions; {@code null} when
 *                         the lower bound is a non-literal — see {@code lowerBoundExpr}
 * @param upperBound       the literal upper bound for BETWEEN conditions; {@code null} when
 *                         the upper bound is a non-literal — see {@code upperBoundExpr}
 * @param valueExpressions the parallel expression list for IN / NOT IN when at least one
 *                         element is a non-literal (parameter ref or dynamic value function
 *                         such as {@link Expression.NowRef}); {@code null} for all-literal lists
 * @param lowerBoundExpr   the lower-bound expression for BETWEEN when the bound is a non-literal
 *                         (parameter ref or dynamic value function such as {@link Expression.NowRef});
 *                         {@code null} when the bound is a literal (captured in {@code lowerBound})
 * @param upperBoundExpr   the upper-bound expression for BETWEEN when the bound is a non-literal
 *                         (parameter ref or dynamic value function such as {@link Expression.NowRef});
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
        List<Expression> valueExpressions,  // IN / NOT IN when any element is non-literal (ParameterRef or NowRef)
        Expression lowerBoundExpr,          // BETWEEN lower bound when bound is non-literal (ParameterRef or NowRef)
        Expression upperBoundExpr           // BETWEEN upper bound when bound is non-literal (ParameterRef or NowRef)
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
        NOT_LIKE,
        /**
         * A scalar-membership test on an array field (CONTAINS keyword operator).
         */
        CONTAINS,
        /**
         * Array contains-all (PostgreSQL {@code @>}).
         */
        ARRAY_CONTAINS,
        /**
         * Array contained-by (PostgreSQL {@code <@}).
         */
        ARRAY_CONTAINED_BY,
        /**
         * Array overlap (PostgreSQL {@code &&}).
         */
        ARRAY_OVERLAP,
        /**
         * Structural equality between an array column and an ARRAY[…] literal.
         */
        ARRAY_EQUALS,
        /**
         * Negation of ARRAY_EQUALS.
         */
        ARRAY_NOT_EQUALS
    }
}
