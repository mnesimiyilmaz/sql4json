package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.List;

/**
 * Handles IN and NOT IN conditions.
 * Evaluates whether a column value is contained in (or absent from) a list of values.
 */
public final class InConditionHandler implements ConditionHandler {

    /**
     * Creates a new instance.
     */
    InConditionHandler() {
    }


    @Override
    public boolean canHandle(ConditionContext ctx) {
        return ctx.type() == ConditionContext.ConditionType.IN
                || ctx.type() == ConditionContext.ConditionType.NOT_IN;
    }

    @Override
    public CriteriaNode handle(ConditionContext ctx,
                               OperatorRegistry operators,
                               FunctionRegistry functions) {
        List<SqlValue> valueList = ctx.valueList();
        boolean negate = ctx.type() == ConditionContext.ConditionType.NOT_IN;
        Expression lhs = ctx.lhsExpression();

        return row -> {
            SqlValue fieldVal = ExpressionEvaluator.evaluate(lhs, row, functions);
            if (fieldVal.isNull()) return false;
            boolean found = valueList.stream()
                    .filter(v -> !v.isNull())
                    .anyMatch(v -> SqlValueComparator.compare(fieldVal, v) == 0);
            return negate != found;
        };
    }
}
