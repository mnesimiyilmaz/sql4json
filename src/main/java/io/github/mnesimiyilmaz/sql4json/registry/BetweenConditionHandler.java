package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.function.BiPredicate;

/**
 * Handles BETWEEN and NOT BETWEEN conditions.
 * Evaluates whether a column value falls within (or outside) an inclusive range.
 */
public final class BetweenConditionHandler implements ConditionHandler {

    /**
     * Creates a new instance.
     */
    BetweenConditionHandler() {
    }


    @Override
    public boolean canHandle(ConditionContext ctx) {
        return ctx.type() == ConditionContext.ConditionType.BETWEEN
                || ctx.type() == ConditionContext.ConditionType.NOT_BETWEEN;
    }

    @Override
    public CriteriaNode handle(ConditionContext ctx,
                               OperatorRegistry operators,
                               FunctionRegistry functions) {
        SqlValue lower = ctx.lowerBound();
        SqlValue upper = ctx.upperBound();
        boolean negate = ctx.type() == ConditionContext.ConditionType.NOT_BETWEEN;
        Expression lhs = ctx.lhsExpression();

        BiPredicate<SqlValue, SqlValue> gte = operators.getPredicate(">=");
        BiPredicate<SqlValue, SqlValue> lte = operators.getPredicate("<=");

        return row -> {
            SqlValue fieldVal = ExpressionEvaluator.evaluate(lhs, row, functions);
            if (fieldVal.isNull() || lower.isNull() || upper.isNull()) return false;
            boolean inRange = gte.test(fieldVal, lower) && lte.test(fieldVal, upper);
            return negate != inRange;
        };
    }
}
