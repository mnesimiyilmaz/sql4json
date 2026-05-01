package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonValueTest {

    @Test
    void jsonNull_isSingleton() {
        assertSame(JsonNullValue.INSTANCE, JsonNullValue.INSTANCE);
    }

    @Test
    void jsonNull_isNull() {
        assertTrue(JsonNullValue.INSTANCE.isNull());
        assertFalse(JsonNullValue.INSTANCE.isObject());
        assertFalse(JsonNullValue.INSTANCE.isArray());
    }

    @Test
    void jsonString_value() {
        JsonStringValue s = new JsonStringValue("hello");
        assertTrue(s.isString());
        assertEquals("hello", s.asString().orElseThrow());
        assertFalse(s.isNull());
    }

    @Test
    void jsonNumber_value() {
        JsonNumberValue n = new JsonLongValue(42L);
        assertTrue(n.isNumber());
        assertEquals(42, n.asNumber().orElseThrow().intValue());
    }

    @Test
    void jsonBoolean_value() {
        JsonBooleanValue b = new JsonBooleanValue(true);
        assertTrue(b.isBoolean());
        assertEquals(true, b.asBoolean().orElseThrow());
    }

    @Test
    void jsonObject_value() {
        JsonObjectValue obj = new JsonObjectValue(Map.of("key", new JsonStringValue("val")));
        assertTrue(obj.isObject());
        assertFalse(obj.isArray());
        assertTrue(obj.asObject().isPresent());
        assertEquals("val", ((JsonStringValue) obj.asObject().get().get("key")).value());
    }

    @Test
    void jsonArray_value() {
        JsonArrayValue arr = new JsonArrayValue(List.of(new JsonLongValue(1L), new JsonLongValue(2L)));
        assertTrue(arr.isArray());
        assertFalse(arr.isObject());
        assertEquals(2, arr.asArray().orElseThrow().size());
    }

    @Test
    void wrongType_returnsEmptyOptional() {
        JsonStringValue s = new JsonStringValue("test");
        assertTrue(s.asNumber().isEmpty());
        assertTrue(s.asBoolean().isEmpty());
        assertTrue(s.asArray().isEmpty());
        assertTrue(s.asObject().isEmpty());
    }

    @Test
    void jsonValue_typeAssignability() {
        JsonValue v = new JsonStringValue("hello");
        assertTrue(v.isString());
        assertFalse(v.isNull());
    }

    // ── Exhaustive type-check coverage for all JsonValue subtypes ────────

    @Test
    void jsonNull_allTypeChecks() {
        JsonNullValue v = JsonNullValue.INSTANCE;
        assertTrue(v.isNull());
        assertFalse(v.isObject());
        assertFalse(v.isArray());
        assertFalse(v.isNumber());
        assertFalse(v.isString());
        assertFalse(v.isBoolean());
        assertTrue(v.asObject().isEmpty());
        assertTrue(v.asArray().isEmpty());
        assertTrue(v.asNumber().isEmpty());
        assertTrue(v.asString().isEmpty());
        assertTrue(v.asBoolean().isEmpty());
    }

    @Test
    void jsonArray_allTypeChecks() {
        JsonArrayValue v = new JsonArrayValue(List.of(new JsonLongValue(1L)));
        assertFalse(v.isNull());
        assertFalse(v.isObject());
        assertTrue(v.isArray());
        assertFalse(v.isNumber());
        assertFalse(v.isString());
        assertFalse(v.isBoolean());
        assertTrue(v.asObject().isEmpty());
        assertTrue(v.asArray().isPresent());
        assertTrue(v.asNumber().isEmpty());
        assertTrue(v.asString().isEmpty());
        assertTrue(v.asBoolean().isEmpty());
    }

    @Test
    void jsonObject_allTypeChecks() {
        JsonObjectValue v = new JsonObjectValue(Map.of("k", new JsonStringValue("v")));
        assertFalse(v.isNull());
        assertTrue(v.isObject());
        assertFalse(v.isArray());
        assertFalse(v.isNumber());
        assertFalse(v.isString());
        assertFalse(v.isBoolean());
        assertTrue(v.asObject().isPresent());
        assertTrue(v.asArray().isEmpty());
        assertTrue(v.asNumber().isEmpty());
        assertTrue(v.asString().isEmpty());
        assertTrue(v.asBoolean().isEmpty());
    }

    @Test
    void jsonNumber_allTypeChecks() {
        JsonNumberValue v = new JsonLongValue(42L);
        assertFalse(v.isNull());
        assertFalse(v.isObject());
        assertFalse(v.isArray());
        assertTrue(v.isNumber());
        assertFalse(v.isString());
        assertFalse(v.isBoolean());
        assertTrue(v.asObject().isEmpty());
        assertTrue(v.asArray().isEmpty());
        assertTrue(v.asNumber().isPresent());
        assertTrue(v.asString().isEmpty());
        assertTrue(v.asBoolean().isEmpty());
    }

    @Test
    void jsonString_allTypeChecks() {
        JsonStringValue v = new JsonStringValue("hello");
        assertFalse(v.isNull());
        assertFalse(v.isObject());
        assertFalse(v.isArray());
        assertFalse(v.isNumber());
        assertTrue(v.isString());
        assertFalse(v.isBoolean());
        assertTrue(v.asObject().isEmpty());
        assertTrue(v.asArray().isEmpty());
        assertTrue(v.asNumber().isEmpty());
        assertTrue(v.asString().isPresent());
        assertTrue(v.asBoolean().isEmpty());
    }

    @Test
    void jsonBoolean_allTypeChecks() {
        JsonBooleanValue v = new JsonBooleanValue(true);
        assertFalse(v.isNull());
        assertFalse(v.isObject());
        assertFalse(v.isArray());
        assertFalse(v.isNumber());
        assertFalse(v.isString());
        assertTrue(v.isBoolean());
        assertTrue(v.asObject().isEmpty());
        assertTrue(v.asArray().isEmpty());
        assertTrue(v.asNumber().isEmpty());
        assertTrue(v.asString().isEmpty());
        assertTrue(v.asBoolean().isPresent());
    }

    @Test
    void jsonNumber_sealedExhaustiveSwitch() {
        JsonValue[] numbers = {
                new JsonLongValue(1L),
                new JsonDoubleValue(1.5),
                new JsonDecimalValue(new java.math.BigDecimal("1.000000000000000001"))
        };
        for (JsonValue n : numbers) {
            String tag = switch (n) {
                case JsonLongValue ignored    -> "long";
                case JsonDoubleValue ignored  -> "double";
                case JsonDecimalValue ignored -> "decimal";
                default                       -> "other";
            };
            assertNotEquals("other", tag);
            assertTrue(n.isNumber());
        }
    }
}
