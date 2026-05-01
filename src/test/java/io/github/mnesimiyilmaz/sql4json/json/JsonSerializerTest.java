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
        assertEquals("42", JsonSerializer.serialize(new JsonLongValue(42L)));
    }

    @Test
    void serialize_long() {
        assertEquals("3000000000", JsonSerializer.serialize(new JsonLongValue(3_000_000_000L)));
    }

    @Test
    void serialize_double() {
        assertEquals("3.14", JsonSerializer.serialize(new JsonDoubleValue(3.14)));
    }

    @Test
    void serialize_bigdecimal() {
        assertEquals("12345678901234567890",
                JsonSerializer.serialize(new JsonDecimalValue(new BigDecimal("12345678901234567890"))));
    }

    @Test
    void serialize_bigdecimal_uses_scientific_notation() {
        // toString() gives "1E+2" — valid JSON and safe (toPlainString is a DoS vector)
        assertEquals("1E+2",
                JsonSerializer.serialize(new JsonDecimalValue(new BigDecimal("1E+2"))));
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
        fields.put("age", new JsonLongValue(30L));
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
                new JsonLongValue(1L),
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
        var inner = new JsonObjectValue(Map.of("id", new JsonLongValue(1L)));
        var arr = new JsonArrayValue(List.of(inner));
        var outer = new JsonObjectValue(Map.of("items", arr));
        assertEquals("{\"items\":[{\"id\":1}]}", JsonSerializer.serialize(outer));
    }

    // ── Pretty-print ────────────────────────────────────────────────────────

    @Test
    void prettySerialize_scalar_isPlainValue() {
        assertEquals("42", JsonSerializer.prettySerialize(new JsonLongValue(42L)));
        assertEquals("3.14", JsonSerializer.prettySerialize(new JsonDoubleValue(3.14)));
        assertEquals("12345678901234567890",
                JsonSerializer.prettySerialize(
                        new JsonDecimalValue(new BigDecimal("12345678901234567890"))));
        assertEquals("\"hi\"", JsonSerializer.prettySerialize(new JsonStringValue("hi")));
        assertEquals("true", JsonSerializer.prettySerialize(new JsonBooleanValue(true)));
        assertEquals("null", JsonSerializer.prettySerialize(JsonNullValue.INSTANCE));
    }

    @Test
    void prettySerialize_emptyObject_isCompact() {
        assertEquals("{}", JsonSerializer.prettySerialize(new JsonObjectValue(Collections.emptyMap())));
    }

    @Test
    void prettySerialize_emptyArray_isCompact() {
        assertEquals("[]", JsonSerializer.prettySerialize(new JsonArrayValue(Collections.emptyList())));
    }

    @Test
    void prettySerialize_object_indentsTwoSpaces() {
        Map<String, JsonValue> fields = new LinkedHashMap<>();
        fields.put("name", new JsonStringValue("Alice"));
        fields.put("age", new JsonLongValue(30L));
        String result = JsonSerializer.prettySerialize(new JsonObjectValue(fields));
        assertEquals("{\n  \"name\": \"Alice\",\n  \"age\": 30\n}", result);
    }

    @Test
    void prettySerialize_arrayOfPrimitives_oneElementPerLine() {
        JsonArrayValue arr = new JsonArrayValue(List.of(
                new JsonLongValue(1L),
                new JsonLongValue(2L),
                new JsonLongValue(3L)));
        assertEquals("[\n  1,\n  2,\n  3\n]", JsonSerializer.prettySerialize(arr));
    }

    @Test
    void prettySerialize_nested_indentsConsistently() {
        Map<String, JsonValue> innerFields = new LinkedHashMap<>();
        innerFields.put("id", new JsonLongValue(1L));
        JsonObjectValue inner = new JsonObjectValue(innerFields);
        JsonArrayValue arr = new JsonArrayValue(List.of(inner));
        Map<String, JsonValue> outerFields = new LinkedHashMap<>();
        outerFields.put("items", arr);
        JsonObjectValue outer = new JsonObjectValue(outerFields);
        String expected =
                "{\n" +
                "  \"items\": [\n" +
                "    {\n" +
                "      \"id\": 1\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        assertEquals(expected, JsonSerializer.prettySerialize(outer));
    }

    @Test
    void prettySerialize_string_escapesSameAsCompact() {
        assertEquals("\"a\\nb\\\"c\"",
                JsonSerializer.prettySerialize(new JsonStringValue("a\nb\"c")));
    }

    @Test
    void prettySerialize_roundtripsThroughParser() {
        String json = "{\"users\":[{\"name\":\"Alice\",\"age\":30}],\"count\":1}";
        JsonValue parsed = JsonParser.parse(json);
        String pretty = JsonSerializer.prettySerialize(parsed);
        JsonValue reparsed = JsonParser.parse(pretty);
        assertEquals(parsed, reparsed);
    }
}
