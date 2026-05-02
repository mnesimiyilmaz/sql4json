// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JSON object value.
 *
 * @param fields the map of field names to their values
 */
public record JsonObjectValue(Map<String, JsonValue> fields) implements JsonValue {
    public boolean isNull() {
        return false;
    }

    public boolean isObject() {
        return true;
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
        return Optional.of(fields);
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
