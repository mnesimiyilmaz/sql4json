package io.github.mnesimiyilmaz.sql4json.grammar;

/**
 * Classification of a built-in function for IDE grouping and colouring.
 *
 * <p>A single function maps to exactly one {@code Category} in
 * {@link FunctionInfo#category()}. When a function could belong to multiple
 * categories (e.g. {@code NOW} as both {@link #DATE_TIME} and {@link #VALUE}),
 * it is tagged with the category most useful for IDE grouping; see the
 * v1.2.0 design spec for per-function decisions.
 *
 * @since 1.2.0
 */
public enum Category {

    /**
     * String-manipulation functions (LOWER, UPPER, CONCAT, ...).
     */
    STRING,

    /**
     * Numeric functions (ABS, ROUND, CEIL, ...).
     */
    MATH,

    /**
     * Date/time functions (TO_DATE, YEAR, DATE_ADD, NOW, ...).
     */
    DATE_TIME,

    /**
     * Type conversion and null handling (CAST, NULLIF, COALESCE).
     */
    CONVERSION,

    /**
     * Aggregate reducers (COUNT, SUM, AVG, MIN, MAX).
     */
    AGGREGATE,

    /**
     * Window functions (ROW_NUMBER, RANK, LAG, ...).
     */
    WINDOW,

    /**
     * Reserved for future zero-argument value functions that do not fit a
     * domain category. Not used in 1.2.0 (NOW is categorised as
     * {@link #DATE_TIME}).
     */
    VALUE
}
