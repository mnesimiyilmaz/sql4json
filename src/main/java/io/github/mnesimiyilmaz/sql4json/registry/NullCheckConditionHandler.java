// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

/**
 * Handles IS NULL and IS NOT NULL conditions by evaluating whether the left-hand expression resolves to a null value.
 */
public final class NullCheckConditionHandler implements ConditionHandler {

    /** Creates a new {@code NullCheckConditionHandler}. */
    public NullCheckConditionHandler() {
        // Stateless handler; behaviour is provided by canHandle/handle.
    }

    @Override
    public boolean canHandle(ConditionContext ctx) {
        return ctx.type() == ConditionContext.ConditionType.IS_NULL
                || ctx.type() == ConditionContext.ConditionType.IS_NOT_NULL;
    }

    @Override
    public CriteriaNode handle(ConditionContext ctx, OperatorRegistry operators, FunctionRegistry functions) {
        Expression lhs = ctx.lhsExpression();
        boolean checkNull = ctx.type() == ConditionContext.ConditionType.IS_NULL;
        return row -> {
            SqlValue value = ExpressionEvaluator.evaluate(lhs, row, functions);
            return checkNull == value.isNull();
        };
    }
}
