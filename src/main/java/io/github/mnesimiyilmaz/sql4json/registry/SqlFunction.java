package io.github.mnesimiyilmaz.sql4json.registry;

/**
 * Sealed base for all SQL function types: scalar, value, and aggregate.
 */
public sealed interface SqlFunction permits ScalarFunction, ValueFunction, AggregateFunction {
    /**
     * Returns the function name (case-insensitive).
     *
     * @return the function name
     */
    String name();
}
