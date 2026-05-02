// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.function.BiPredicate;

/**
 * Handles standard comparison conditions ({@code =, !=, <, >, <=, >=}). Delegates operator evaluation to the
 * {@link OperatorRegistry}.
 */
public final class ComparisonConditionHandler implements ConditionHandler {

    /** Creates a new instance. */
    ComparisonConditionHandler() {}

    @Override
    public boolean canHandle(ConditionContext ctx) {
        return ctx.type() == ConditionContext.ConditionType.COMPARISON;
    }

    @Override
    public CriteriaNode handle(ConditionContext ctx, OperatorRegistry operators, FunctionRegistry functions) {
        BiPredicate<SqlValue, SqlValue> predicate = operators.getPredicate(ctx.operator());
        Expression lhs = ctx.lhsExpression();
        Expression rhs = ctx.rhsExpression();

        return row -> {
            SqlValue lhsVal = evaluateExpr(lhs, row, functions);
            SqlValue rhsVal = evaluateExpr(rhs, row, functions);
            if (lhsVal.isNull() || rhsVal.isNull()) return false;
            return predicate.test(lhsVal, rhsVal);
        };
    }

    private static SqlValue evaluateExpr(Expression expr, RowAccessor row, FunctionRegistry functions) {
        if (expr.containsAggregate()) {
            return row.sourceGroup()
                    .map(group -> ExpressionEvaluator.evaluateAggregate(expr, group, functions))
                    .orElseGet(() -> ExpressionEvaluator.evaluate(expr, row, functions));
        }
        return ExpressionEvaluator.evaluate(expr, row, functions);
    }
}
