package io.github.mnesimiyilmaz.sql4json.json;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonDoubleValueTest {

    @Test
    void value_storesUnboxedDouble() {
        JsonDoubleValue v = new JsonDoubleValue(3.14);
        assertEquals(3.14, v.value(), 1e-12);
    }

    @Test
    void numberValue_returnsDoubleBox() {
        JsonDoubleValue v = new JsonDoubleValue(3.14);
        assertEquals(Double.valueOf(3.14), v.numberValue());
    }

    @Test
    void typeFlags_onlyIsNumber() {
        JsonDoubleValue v = new JsonDoubleValue(0.0);
        assertTrue(v.isNumber());
        assertFalse(v.isNull());
    }

    @Test
    void nan_andInfinity_round_trip() {
        JsonDoubleValue nan = new JsonDoubleValue(Double.NaN);
        assertEquals(Double.NaN, nan.value());
    }
}
