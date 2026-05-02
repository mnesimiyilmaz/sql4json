// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.ExpressionEvaluator;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonToSqlConverter;
import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Handles {@code CONTAINS} (scalar-membership) and the array-set operators {@code @>}, {@code <@}, {@code &&},
 * {@code ARRAY_EQUALS}, {@code ARRAY_NOT_EQUALS}.
 *
 * <p>Arrays are read directly from a {@link RowAccessor}'s original {@code JsonValue} (or via flat-key reassembly for
 * post-JOIN rows) — see {@link ArrayPathNavigator}. No {@code SqlArray} type exists; equality is delegated to
 * {@link SqlValueComparator} — same as {@code IN}.
 *
 * @since 1.2.0
 */
final class ArrayPredicateConditionHandler implements ConditionHandler {

    private static final Set<ConditionContext.ConditionType> SUPPORTED = EnumSet.of(
            ConditionContext.ConditionType.CONTAINS,
            ConditionContext.ConditionType.ARRAY_CONTAINS,
            ConditionContext.ConditionType.ARRAY_CONTAINED_BY,
            ConditionContext.ConditionType.ARRAY_OVERLAP,
            ConditionContext.ConditionType.ARRAY_EQUALS,
            ConditionContext.ConditionType.ARRAY_NOT_EQUALS);

    @Override
    public boolean canHandle(ConditionContext ctx) {
        return SUPPORTED.contains(ctx.type());
    }

    @Override
    public CriteriaNode handle(ConditionContext ctx, OperatorRegistry operators, FunctionRegistry functions) {
        var type = ctx.type();
        Expression lhs = ctx.lhsExpression();
        String lhsPath = lhs.innermostColumnPath();

        if (type == ConditionContext.ConditionType.CONTAINS) {
            return row -> evaluateContains(ctx, lhsPath, row, functions);
        }
        return row -> evaluateArraySetPredicate(ctx, lhsPath, type, row, functions);
    }

    private boolean evaluateArraySetPredicate(
            ConditionContext ctx,
            String lhsPath,
            ConditionContext.ConditionType type,
            RowAccessor row,
            FunctionRegistry functions) {
        JsonArrayValue lhsArr = ArrayPathNavigator.navigateToArray(row, lhsPath);
        if (lhsArr == null) {
            return false;
        }
        Optional<List<SqlValue>> resolved = resolveRhsArray(ctx, row, functions);
        if (resolved.isEmpty()) {
            // RHS column-ref or parameter resolved to non-array
            return false;
        }
        List<SqlValue> rhs = resolved.get();
        return switch (type) {
            case ARRAY_CONTAINS -> containsAll(lhsArr, rhs);
            case ARRAY_CONTAINED_BY -> containedBy(lhsArr, rhs);
            case ARRAY_OVERLAP -> overlap(lhsArr, rhs);
            case ARRAY_EQUALS -> structuralEquals(lhsArr, rhs);
            case ARRAY_NOT_EQUALS -> !structuralEquals(lhsArr, rhs);
            default -> throw new SQL4JsonExecutionException("unreachable: " + type);
        };
    }

    private Optional<List<SqlValue>> resolveRhsArray(
            ConditionContext ctx, RowAccessor row, FunctionRegistry functions) {
        // Branch 1 — array literal: valueExpressions is non-null, evaluate each element.
        if (ctx.valueExpressions() != null) {
            return Optional.of(ctx.valueExpressions().stream()
                    .map(e -> ExpressionEvaluator.evaluate(e, row, functions))
                    .toList());
        }
        // Branches 2-3 — column-ref or bare parameter: rhsExpression carries the source.
        return switch (ctx.rhsExpression()) {
            case null ->
                throw new SQL4JsonExecutionException(
                        "array predicate has no RHS expression — listener bug for type " + ctx.type());
            case Expression.ColumnRef colRef -> resolveColumnRefArray(row, colRef);
            case Expression.ParameterRef ignored ->
                throw new SQL4JsonExecutionException(
                        "ParameterRef in array RHS reached handler — ParameterSubstitutor should have replaced it");
            // Fallback: evaluate as scalar, wrap into single-element array.
            default -> resolveScalarFallback(ctx.rhsExpression(), row, functions);
        };
    }

    private static Optional<List<SqlValue>> resolveColumnRefArray(RowAccessor row, Expression.ColumnRef colRef) {
        JsonArrayValue arr = ArrayPathNavigator.navigateToArray(row, colRef.innermostColumnPath());
        if (arr == null) {
            return Optional.empty();
        }
        return Optional.of(
                arr.elements().stream().map(JsonToSqlConverter::toSqlValueSafe).toList());
    }

    private static Optional<List<SqlValue>> resolveScalarFallback(
            Expression rhs, RowAccessor row, FunctionRegistry functions) {
        SqlValue scalar = ExpressionEvaluator.evaluate(rhs, row, functions);
        return scalar.isNull() ? Optional.empty() : Optional.of(List.of(scalar));
    }

    private static boolean containsAll(JsonArrayValue haystack, List<SqlValue> needles) {
        if (needles.isEmpty()) {
            return true;
        }
        List<SqlValue> hay = haystack.elements().stream()
                .map(JsonToSqlConverter::toSqlValueSafe)
                .toList();
        for (SqlValue needle : needles) {
            if (needle.isNull()) {
                return false;
            }
            boolean found =
                    hay.stream().filter(v -> !v.isNull()).anyMatch(v -> SqlValueComparator.compare(v, needle) == 0);
            if (!found) {
                return false;
            }
        }
        return true;
    }

    private static boolean containedBy(JsonArrayValue lhsArr, List<SqlValue> rhs) {
        if (lhsArr.elements().isEmpty()) {
            return true;
        }
        for (JsonValue elem : lhsArr.elements()) {
            SqlValue ev = JsonToSqlConverter.toSqlValueSafe(elem);
            if (ev.isNull()) {
                return false;
            }
            boolean found = rhs.stream().filter(v -> !v.isNull()).anyMatch(v -> SqlValueComparator.compare(ev, v) == 0);
            if (!found) {
                return false;
            }
        }
        return true;
    }

    private static boolean structuralEquals(JsonArrayValue lhsArr, List<SqlValue> rhs) {
        if (lhsArr.elements().size() != rhs.size()) {
            return false;
        }
        for (int i = 0; i < rhs.size(); i++) {
            SqlValue lv = JsonToSqlConverter.toSqlValueSafe(lhsArr.elements().get(i));
            SqlValue rv = rhs.get(i);
            if (lv.isNull() || rv.isNull()) {
                return false; // SQL-standard: NULL == anything → false
            }
            if (SqlValueComparator.compare(lv, rv) != 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean overlap(JsonArrayValue lhsArr, List<SqlValue> rhs) {
        if (rhs.isEmpty()) {
            return false;
        }
        List<SqlValue> hay = lhsArr.elements().stream()
                .map(JsonToSqlConverter::toSqlValueSafe)
                .toList();
        for (SqlValue r : rhs) {
            if (r.isNull()) {
                continue;
            }
            boolean any = hay.stream().filter(v -> !v.isNull()).anyMatch(v -> SqlValueComparator.compare(v, r) == 0);
            if (any) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateContains(
            ConditionContext ctx, String lhsPath, RowAccessor row, FunctionRegistry functions) {
        JsonArrayValue arr = ArrayPathNavigator.navigateToArray(row, lhsPath);
        if (arr == null) {
            return false;
        }
        SqlValue needle = resolveScalarRhs(ctx, row, functions);
        if (needle.isNull()) {
            return false;
        }
        for (JsonValue elem : arr.elements()) {
            SqlValue ev = JsonToSqlConverter.toSqlValueSafe(elem);
            if (ev.isNull()) {
                continue;
            }
            if (SqlValueComparator.compare(ev, needle) == 0) {
                return true;
            }
        }
        return false;
    }

    private SqlValue resolveScalarRhs(ConditionContext ctx, RowAccessor row, FunctionRegistry functions) {
        if (ctx.testValue() != null) {
            return ctx.testValue();
        }
        if (ctx.rhsExpression() != null) {
            return ExpressionEvaluator.evaluate(ctx.rhsExpression(), row, functions);
        }
        throw new SQL4JsonExecutionException("CONTAINS condition has no scalar RHS — listener bug");
    }
}
