/**
 * SQL and JSON value type hierarchies for sql4json.
 *
 * <p>This package contains two sealed type hierarchies:
 *
 * <ul>
 *   <li>{@link io.github.mnesimiyilmaz.sql4json.types.JsonValue} &mdash; a JSON abstraction used as both input and
 *       output. Returned by {@link io.github.mnesimiyilmaz.sql4json.SQL4Json#queryAsJsonValue}.
 *   <li>{@link io.github.mnesimiyilmaz.sql4json.types.SqlValue} &mdash; typed SQL values used internally during query
 *       evaluation (comparisons, aggregation, functions).
 * </ul>
 */
package io.github.mnesimiyilmaz.sql4json.types;
