package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.function.Supplier;

/**
 * A zero-argument value-producing SQL function (e.g. {@code NOW()}).
 *
 * @param name  function name (case-insensitive)
 * @param apply supplier that produces the value on each invocation
 */
public record ValueFunction(
        String name,
        Supplier<SqlValue> apply
) implements SqlFunction {
}
