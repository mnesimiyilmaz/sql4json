package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JSON boolean value.
 *
 * @param value the boolean value
 */
public record JsonBooleanValue(boolean value) implements JsonValue {
    public static final JsonBooleanValue TRUE  = new JsonBooleanValue(true);
    public static final JsonBooleanValue FALSE = new JsonBooleanValue(false);

    public boolean isNull() {
        return false;
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
        return true;
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
        return Optional.of(value);
    }
}
