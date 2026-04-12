package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonSerializerTest {

    // ── Primitives ──────────────────────────────────────────────────────

    @Test
    void serialize_string() {
        assertEquals("\"hello\"", JsonSerializer.serialize(new JsonStringValue("hello")));
    }

    @Test
    void serialize_string_with_special_chars() {
        String result = JsonSerializer.serialize(new JsonStringValue("line1\nline2\ttab\\backslash\"quote"));
        assertEquals("\"line1\\nline2\\ttab\\\\backslash\\\"quote\"", result);
    }

    @Test
    void serialize_string_with_control_chars() {
        String result = JsonSerializer.serialize(new JsonStringValue("\u0001\u001f"));
        assertEquals("\"\\u0001\\u001f\"", result);
    }

    @Test
    void serialize_integer() {
        assertEquals("42", JsonSerializer.serialize(new JsonNumberValue(42)));
    }

    @Test
    void serialize_long() {
        assertEquals("3000000000", JsonSerializer.serialize(new JsonNumberValue(3_000_000_000L)));
    }

    @Test
    void serialize_double() {
        assertEquals("3.14", JsonSerializer.serialize(new JsonNumberValue(3.14)));
    }

    @Test
    void serialize_bigdecimal() {
        assertEquals("12345678901234567890",
                JsonSerializer.serialize(new JsonNumberValue(new BigDecimal("12345678901234567890"))));
    }

    @Test
    void serialize_bigdecimal_uses_scientific_notation() {
        // toString() gives "1E+2" — valid JSON and safe (toPlainString is a DoS vector)
        assertEquals("1E+2",
                JsonSerializer.serialize(new JsonNumberValue(new BigDecimal("1E+2"))));
    }

    @Test
    void serialize_boolean_true() {
        assertEquals("true", JsonSerializer.serialize(new JsonBooleanValue(true)));
    }

    @Test
    void serialize_boolean_false() {
        assertEquals("false", JsonSerializer.serialize(new JsonBooleanValue(false)));
    }

    @Test
    void serialize_null() {
        assertEquals("null", JsonSerializer.serialize(JsonNullValue.INSTANCE));
    }

    // ── Objects ──────────────────────────────────────────────────────────

    @Test
    void serialize_empty_object() {
        assertEquals("{}", JsonSerializer.serialize(new JsonObjectValue(Collections.emptyMap())));
    }

    @Test
    void serialize_object_with_fields() {
        Map<String, JsonValue> fields = new LinkedHashMap<>();
        fields.put("name", new JsonStringValue("Alice"));
        fields.put("age", new JsonNumberValue(30));
        assertEquals("{\"name\":\"Alice\",\"age\":30}",
                JsonSerializer.serialize(new JsonObjectValue(fields)));
    }

    // ── Arrays ──────────────────────────────────────────────────────────

    @Test
    void serialize_empty_array() {
        assertEquals("[]", JsonSerializer.serialize(new JsonArrayValue(Collections.emptyList())));
    }

    @Test
    void serialize_array_with_elements() {
        List<JsonValue> elems = List.of(
                new JsonNumberValue(1),
                new JsonStringValue("two"),
                new JsonBooleanValue(true),
                JsonNullValue.INSTANCE);
        assertEquals("[1,\"two\",true,null]", JsonSerializer.serialize(new JsonArrayValue(elems)));
    }

    // ── Roundtrip ───────────────────────────────────────────────────────

    @Test
    void roundtrip_complex_structure() {
        String json = "{\"users\":[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}],\"count\":2}";
        JsonValue parsed = JsonParser.parse(json);
        String serialized = JsonSerializer.serialize(parsed);
        JsonValue reparsed = JsonParser.parse(serialized);
        assertEquals(parsed, reparsed);
    }

    @Test
    void roundtrip_unicode_string() {
        String json = "{\"city\":\"\\u6771\\u4EAC\"}";
        JsonValue parsed = JsonParser.parse(json);
        String serialized = JsonSerializer.serialize(parsed);
        JsonValue reparsed = JsonParser.parse(serialized);
        assertEquals(parsed, reparsed);
    }

    // ── Additional edge cases for coverage ─────────────────────────────────

    @Test
    void serialize_float() {
        assertEquals("3.14", JsonSerializer.serialize(new JsonNumberValue(3.14f)));
    }

    @Test
    void serialize_string_with_backspace_and_formfeed() {
        assertEquals("\"\\b\\f\"", JsonSerializer.serialize(new JsonStringValue("\b\f")));
    }

    @Test
    void serialize_string_with_carriage_return() {
        assertEquals("\"\\r\"", JsonSerializer.serialize(new JsonStringValue("\r")));
    }

    @Test
    void serialize_string_with_null_byte() {
        assertEquals("\"\\u0000\"", JsonSerializer.serialize(new JsonStringValue("\u0000")));
    }

    @Test
    void serialize_nested_object_in_array() {
        var inner = new JsonObjectValue(Map.of("id", new JsonNumberValue(1)));
        var arr = new JsonArrayValue(List.of(inner));
        var outer = new JsonObjectValue(Map.of("items", arr));
        assertEquals("{\"items\":[{\"id\":1}]}", JsonSerializer.serialize(outer));
    }
}
