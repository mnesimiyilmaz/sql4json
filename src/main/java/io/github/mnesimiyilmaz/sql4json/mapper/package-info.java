/**
 * Internal object-mapping implementation. This package is intentionally not exported
 * from the JPMS module — callers interact with it only through the public API
 * ({@link io.github.mnesimiyilmaz.sql4json.SQL4Json#queryAs},
 * {@link io.github.mnesimiyilmaz.sql4json.PreparedQuery#executeAs},
 * {@link io.github.mnesimiyilmaz.sql4json.SQL4JsonEngine#queryAs},
 * and {@link io.github.mnesimiyilmaz.sql4json.types.JsonValue#as}).
 *
 * <p>Keeping the implementation internal gives us freedom to evolve the mapper
 * without API churn.
 */
package io.github.mnesimiyilmaz.sql4json.mapper;
