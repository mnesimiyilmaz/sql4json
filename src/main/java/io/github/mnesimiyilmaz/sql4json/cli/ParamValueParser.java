package io.github.mnesimiyilmaz.sql4json.cli;

import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonBooleanValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonNullValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonNumberValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonParser;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses {@code name=<json-literal>} pairs supplied via {@code -p/--param} into
 * {@code (String, Object)} entries that the CLI can pass through to
 * {@link io.github.mnesimiyilmaz.sql4json.BoundParameters#bind(String, Object)}.
 *
 * <p>Accepted JSON literals: string, number, boolean, null, array. Object literals
 * are rejected because objects are not bindable as scalar parameter values.</p>
 *
 * @since 1.2.0
 */
final class ParamValueParser {

    private ParamValueParser() {
        // utility class — no instances
    }

    /**
     * Parses {@code "name=<json>"} into a name/value entry.
     *
     * @param raw the value of a single {@code -p/--param} argument
     * @return entry whose key is the parameter name and whose value is the bound Java value
     * @throws UsageException if {@code raw} is null, missing the {@code =} separator, has an
     *                        empty name, has an empty value, contains malformed JSON, or
     *                        contains a JSON object literal
     */
    static Map.Entry<String, Object> parse(String raw) {
        if (raw == null) {
            throw new UsageException("--param value must not be null");
        }
        int eq = raw.indexOf('=');
        if (eq < 0) {
            throw new UsageException("--param expects name=<json>, got '" + raw + "'");
        }
        String name = raw.substring(0, eq);
        String json = raw.substring(eq + 1);
        if (name.isEmpty()) {
            throw new UsageException("--param name must not be empty");
        }
        if (json.isEmpty()) {
            throw new UsageException("--param '" + name + "' has empty JSON value");
        }
        JsonValue parsed;
        try {
            parsed = JsonParser.parse(json);
        } catch (RuntimeException e) {
            throw new UsageException(
                    "--param '" + name + "' has invalid JSON value: " + e.getMessage());
        }
        return new AbstractMap.SimpleImmutableEntry<>(name, toJavaValue(parsed, name));
    }

    private static Object toJavaValue(JsonValue value, String paramName) {
        return switch (value) {
            case JsonStringValue(var s) -> s;
            case JsonNumberValue n -> n.numberValue();
            case JsonBooleanValue(var b) -> b;
            case JsonNullValue ignored -> null;
            case JsonArrayValue(var elements) -> {
                List<Object> out = new ArrayList<>(elements.size());
                for (JsonValue el : elements) out.add(toJavaValue(el, paramName));
                yield out;
            }
            case JsonObjectValue ignored -> throw new UsageException(
                    "--param '" + paramName + "' cannot bind a JSON object value");
        };
    }
}
