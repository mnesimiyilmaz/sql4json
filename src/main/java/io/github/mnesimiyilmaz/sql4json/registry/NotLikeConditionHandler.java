// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

final class NotLikeConditionHandler implements ConditionHandler {

    private final BoundedPatternCache patternCache;

    NotLikeConditionHandler(BoundedPatternCache cache) {
        this.patternCache = cache;
    }

    @Override
    public boolean canHandle(ConditionContext ctx) {
        return ctx.type() == ConditionContext.ConditionType.NOT_LIKE;
    }

    @Override
    public CriteriaNode handle(ConditionContext ctx, OperatorRegistry operators, FunctionRegistry functions) {
        return LikeConditionHandler.buildPatternNode(
                ctx.lhsExpression(), ctx.rhsExpression(), patternCache, functions, true);
    }
}
