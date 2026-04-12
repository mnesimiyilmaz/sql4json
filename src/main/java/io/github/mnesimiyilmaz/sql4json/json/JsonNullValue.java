package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Represents JSON null as a sealed record type.
 * <p>Use {@link #INSTANCE} — do not call {@code new JsonNullValue()} directly.
 * The singleton property is a convention; the Java record type system does not
 * enforce private construction.
 */
public record JsonNullValue() implements JsonValue {
    public static final JsonNullValue INSTANCE = new JsonNullValue();

    public boolean isNull() {
        return true;
    }

    public boolean isObject() {
        return false;
    }

    public boolean isArray() {
        return false;
    }

    public boolean isNumber() {
        return false;
    }

    public boolean isString() {
        return false;
    }

    public boolean isBoolean() {
        return false;
    }

    public Optional<Map<String, JsonValue>> asObject() {
        return Optional.empty();
    }

    public Optional<List<JsonValue>> asArray() {
        return Optional.empty();
    }

    public Optional<Number> asNumber() {
        return Optional.empty();
    }

    public Optional<String> asString() {
        return Optional.empty();
    }

    public Optional<Boolean> asBoolean() {
        return Optional.empty();
    }
}
