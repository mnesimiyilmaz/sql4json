package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.function.BiPredicate;

/**
 * Definition of a comparison operator (e.g. {@code =}, {@code >=}).
 *
 * @param symbol    the operator symbol
 * @param type      the operator category
 * @param predicate the two-value comparison logic
 */
public record ComparisonOperatorDef(
        String symbol,
        OperatorType type,
        BiPredicate<SqlValue, SqlValue> predicate
) {
}
