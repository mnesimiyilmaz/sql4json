package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Serializes a {@link JsonValue} tree into a compact JSON string.
 */
public final class JsonSerializer {

    private JsonSerializer() {
    }

    /**
     * Serializes the given {@link JsonValue} into a compact JSON string.
     *
     * @param value the JSON value to serialize
     * @return the compact JSON string representation
     */
    public static String serialize(JsonValue value) {
        StringBuilder sb = new StringBuilder();
        writeTo(sb, value);
        return sb.toString();
    }

    static void writeTo(StringBuilder sb, JsonValue value) {
        switch (value) {
            case JsonObjectValue(var fields) -> writeObject(sb, fields);
            case JsonArrayValue(var elements) -> writeArray(sb, elements);
            case JsonStringValue(var val) -> writeString(sb, val);
            case JsonNumberValue(var num) -> writeNumber(sb, num);
            case JsonBooleanValue(var val) -> sb.append(val);
            case JsonNullValue ignored -> sb.append("null");
        }
    }

    private static void writeObject(StringBuilder sb, Map<String, JsonValue> fields) {
        sb.append('{');
        boolean first = true;
        for (var entry : fields.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            writeString(sb, entry.getKey());
            sb.append(':');
            writeTo(sb, entry.getValue());
        }
        sb.append('}');
    }

    private static void writeArray(StringBuilder sb, List<JsonValue> elements) {
        sb.append('[');
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0) sb.append(',');
            writeTo(sb, elements.get(i));
        }
        sb.append(']');
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static void writeNumber(StringBuilder sb, Number num) {
        if (num instanceof BigDecimal bd) {
            // Use toString() (scientific notation) — NOT toPlainString().
            // toPlainString() on a value like BigDecimal("1e999999999") would try to
            // allocate a billion-character string, causing OutOfMemoryError.
            // Scientific notation (e.g., "1.5E+10") is valid JSON per RFC 8259.
            sb.append(bd);
        } else {
            sb.append(num.toString());
        }
    }
}
