package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonLongValueTest {

    @Test
    void value_storesUnboxedLong() {
        JsonLongValue v = new JsonLongValue(42L);
        assertEquals(42L, v.value());
    }

    @Test
    void numberValue_returnsLongBox() {
        JsonLongValue v = new JsonLongValue(99L);
        assertEquals(Long.valueOf(99L), v.numberValue());
    }

    @Test
    void asNumber_present() {
        JsonLongValue v = new JsonLongValue(7L);
        assertTrue(v.asNumber().isPresent());
        assertEquals(7L, v.asNumber().get().longValue());
    }

    @Test
    void typeFlags_onlyIsNumber() {
        JsonLongValue v = new JsonLongValue(0L);
        assertTrue(v.isNumber());
        assertFalse(v.isString());
        assertFalse(v.isObject());
        assertFalse(v.isArray());
        assertFalse(v.isBoolean());
        assertFalse(v.isNull());
    }

    @Test
    void implementsJsonNumberValueAndJsonValue() {
        JsonLongValue v = new JsonLongValue(0L);
        assertInstanceOf(JsonNumberValue.class, v);
        assertInstanceOf(JsonValue.class, v);
    }

    @Test
    void recordEquality_byValue() {
        assertEquals(new JsonLongValue(5L), new JsonLongValue(5L));
        assertNotEquals(new JsonLongValue(5L), new JsonLongValue(6L));
    }
}
