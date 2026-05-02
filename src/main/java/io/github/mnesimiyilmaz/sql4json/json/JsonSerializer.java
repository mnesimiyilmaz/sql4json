// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import java.util.Map;

/** Serializes a {@link JsonValue} tree into a compact or pretty-printed JSON string. */
public final class JsonSerializer {

    private JsonSerializer() {
        // utility class — no instances
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

    /**
     * Serializes the given {@link JsonValue} into a pretty-printed JSON string. Uses two-space indentation and one
     * element per line. Empty objects/arrays remain compact ({@code {}} / {@code []}). Output has no trailing newline.
     *
     * @param value the JSON value to serialize
     * @return the pretty-printed JSON string representation
     * @since 1.2.0
     */
    public static String prettySerialize(JsonValue value) {
        StringBuilder sb = new StringBuilder();
        writePretty(sb, value, 0);
        return sb.toString();
    }

    static void writeTo(StringBuilder sb, JsonValue value) {
        switch (value) {
            case JsonObjectValue(var fields) -> writeObject(sb, fields);
            case JsonArrayValue(var elements) -> writeArray(sb, elements);
            case JsonStringValue(var val) -> writeString(sb, val);
            case JsonLongValue(long lv) -> sb.append(lv);
            case JsonDoubleValue(double dv) -> sb.append(dv);
            case JsonDecimalValue(var bdv) -> sb.append(bdv);
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

    private static void writePretty(StringBuilder sb, JsonValue value, int depth) {
        switch (value) {
            case JsonObjectValue(var fields) -> writePrettyObject(sb, fields, depth);
            case JsonArrayValue(var elements) -> writePrettyArray(sb, elements, depth);
            case JsonStringValue(var val) -> writeString(sb, val);
            case JsonLongValue(long lv) -> sb.append(lv);
            case JsonDoubleValue(double dv) -> sb.append(dv);
            case JsonDecimalValue(var bdv) -> sb.append(bdv);
            case JsonBooleanValue(var val) -> sb.append(val);
            case JsonNullValue ignored -> sb.append("null");
        }
    }

    private static void writePrettyObject(StringBuilder sb, Map<String, JsonValue> fields, int depth) {
        if (fields.isEmpty()) {
            sb.append("{}");
            return;
        }
        sb.append('{');
        boolean first = true;
        for (var entry : fields.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('\n');
            indent(sb, depth + 1);
            writeString(sb, entry.getKey());
            sb.append(": ");
            writePretty(sb, entry.getValue(), depth + 1);
        }
        sb.append('\n');
        indent(sb, depth);
        sb.append('}');
    }

    private static void writePrettyArray(StringBuilder sb, List<JsonValue> elements, int depth) {
        if (elements.isEmpty()) {
            sb.append("[]");
            return;
        }
        sb.append('[');
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('\n');
            indent(sb, depth + 1);
            writePretty(sb, elements.get(i), depth + 1);
        }
        sb.append('\n');
        indent(sb, depth);
        sb.append(']');
    }

    private static void indent(StringBuilder sb, int depth) {
        sb.repeat("  ", Math.max(0, depth));
    }
}
