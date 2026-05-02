// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.regex.Pattern;

final class LikeConditionHandler implements ConditionHandler {

    // Pattern cache — avoids recompiling the same LIKE pattern on every row.
    private final BoundedPatternCache patternCache;

    LikeConditionHandler(BoundedPatternCache cache) {
        this.patternCache = cache;
    }

    @Override
    public boolean canHandle(ConditionContext ctx) {
        return ctx.type() == ConditionContext.ConditionType.LIKE;
    }

    @Override
    public CriteriaNode handle(ConditionContext ctx, OperatorRegistry operators, FunctionRegistry functions) {
        return buildPatternNode(ctx.lhsExpression(), ctx.rhsExpression(), patternCache, functions, false);
    }

    /**
     * Shared LIKE/NOT LIKE pattern matching logic. When {@code negate} is true, the match result is inverted (NOT
     * LIKE).
     */
    static CriteriaNode buildPatternNode(
            Expression lhs, Expression rhs, BoundedPatternCache cache, FunctionRegistry functions, boolean negate) {
        // Static pattern: pre-compile for performance
        if (rhs instanceof Expression.LiteralVal(var val) && val instanceof SqlString(var pat)) {
            Pattern compiled = cache.computeIfAbsent(pat, LikeConditionHandler::compilePattern);
            return row -> {
                SqlValue value = ExpressionEvaluator.evaluate(lhs, row, functions);
                if (!(value instanceof SqlString(var str))) return false;
                return negate != compiled.matcher(str).matches();
            };
        }

        // Dynamic pattern: evaluate per-row (column ref or function result)
        return row -> {
            SqlValue value = ExpressionEvaluator.evaluate(lhs, row, functions);
            if (!(value instanceof SqlString(var str))) return false;
            SqlValue patternVal = ExpressionEvaluator.evaluate(rhs, row, functions);
            if (!(patternVal instanceof SqlString(var patStr))) return false;
            Pattern compiled = cache.computeIfAbsent(patStr, LikeConditionHandler::compilePattern);
            return negate != compiled.matcher(str).matches();
        };
    }

    /**
     * Compiles a SQL LIKE pattern to a regex Pattern.
     *
     * <p>ReDoS safety: every literal character (non-wildcard) is wrapped with Pattern.quote() so regex metacharacters
     * (., +, *, [, ], (, ), etc.) are treated as literals. Only % and _ retain their wildcard meaning.
     *
     * <p>Case-insensitive: LIKE comparisons are case-insensitive per SQL convention.
     */
    static Pattern compilePattern(String likePattern) {
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < likePattern.length(); i++) {
            char c = likePattern.charAt(i);
            switch (c) {
                case '%' -> regex.append(".*");
                case '_' -> regex.append(".");
                default -> regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        regex.append("$");
        return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    }
}
