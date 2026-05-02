// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JSON number value — sealed root of the numeric family.
 *
 * <p>Implementations store primitives unboxed where possible:
 *
 * <ul>
 *   <li>{@link JsonLongValue} — integer values that fit in a {@code long}
 *   <li>{@link JsonDoubleValue} — fractional / exponent values that fit in a {@code double}
 *   <li>{@link JsonDecimalValue} — anything else, kept as {@link java.math.BigDecimal}
 * </ul>
 *
 * <p>{@link #numberValue()} returns a {@link Number} view for callers that need the legacy boxed shape.
 *
 * @since 1.2.0
 */
public sealed interface JsonNumberValue extends JsonValue permits JsonLongValue, JsonDoubleValue, JsonDecimalValue {

    /**
     * Returns this number as a {@link Number}. Boxes the underlying primitive for {@link JsonLongValue} /
     * {@link JsonDoubleValue}; passes the {@link java.math.BigDecimal} through for {@link JsonDecimalValue}.
     *
     * @return the numeric value as a boxed {@link Number}
     */
    Number numberValue();

    @Override
    default boolean isNumber() {
        return true;
    }

    @Override
    default Optional<Number> asNumber() {
        return Optional.of(numberValue());
    }

    @Override
    default boolean isObject() {
        return false;
    }

    @Override
    default boolean isArray() {
        return false;
    }

    @Override
    default boolean isString() {
        return false;
    }

    @Override
    default boolean isBoolean() {
        return false;
    }

    @Override
    default boolean isNull() {
        return false;
    }

    @Override
    default Optional<Map<String, JsonValue>> asObject() {
        return Optional.empty();
    }

    @Override
    default Optional<List<JsonValue>> asArray() {
        return Optional.empty();
    }

    @Override
    default Optional<String> asString() {
        return Optional.empty();
    }

    @Override
    default Optional<Boolean> asBoolean() {
        return Optional.empty();
    }
}
