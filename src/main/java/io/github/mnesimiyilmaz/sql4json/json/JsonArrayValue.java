package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JSON array value.
 *
 * @param elements the list of elements in this array
 */
public record JsonArrayValue(List<JsonValue> elements) implements JsonValue {
    public boolean isNull() {
        return false;
    }

    public boolean isObject() {
        return false;
    }

    public boolean isArray() {
        return true;
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
        return Optional.of(elements);
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
