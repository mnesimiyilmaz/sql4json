// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.List;

/**
 * Handles IN and NOT IN conditions. Evaluates whether a column value is contained in (or absent from) a list of values.
 */
public final class InConditionHandler implements ConditionHandler {

    /** Creates a new instance. */
    InConditionHandler() {}

    @Override
    public boolean canHandle(ConditionContext ctx) {
        return ctx.type() == ConditionContext.ConditionType.IN || ctx.type() == ConditionContext.ConditionType.NOT_IN;
    }

    @Override
    public CriteriaNode handle(ConditionContext ctx, OperatorRegistry operators, FunctionRegistry functions) {
        List<SqlValue> valueList = ctx.valueList();
        List<Expression> valueExprs = ctx.valueExpressions();
        boolean negate = ctx.type() == ConditionContext.ConditionType.NOT_IN;
        Expression lhs = ctx.lhsExpression();

        if (valueList != null) {
            // All-literal path: fast static check against pre-built value list
            return row -> {
                SqlValue fieldVal = ExpressionEvaluator.evaluate(lhs, row, functions);
                if (fieldVal.isNull()) return false;
                boolean found = valueList.stream()
                        .filter(v -> !v.isNull())
                        .anyMatch(v -> SqlValueComparator.compare(fieldVal, v) == 0);
                return negate != found;
            };
        }
        // Expression path: evaluates each element per-row. Only reachable for non-parameterized
        // queries with a non-literal element (e.g. NOW()) — when a BoundParameters substitution
        // runs, ParameterSubstitutor resolves every element (ParameterRef and NowRef) to literals
        // before this handler is invoked, so the all-literal branch above is taken instead.
        return row -> {
            SqlValue fieldVal = ExpressionEvaluator.evaluate(lhs, row, functions);
            if (fieldVal.isNull()) return false;
            boolean found = valueExprs.stream()
                    .map(e -> ExpressionEvaluator.evaluate(e, row, functions))
                    .filter(v -> !v.isNull())
                    .anyMatch(v -> SqlValueComparator.compare(fieldVal, v) == 0);
            return negate != found;
        };
    }
}
