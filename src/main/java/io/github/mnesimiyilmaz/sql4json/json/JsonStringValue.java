package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JSON string value.
 *
 * @param value the string value
 */
public record JsonStringValue(String value) implements JsonValue {
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
        return true;
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
        return Optional.of(value);
    }

    public Optional<Boolean> asBoolean() {
        return Optional.empty();
    }
}
