// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class JsonDecimalValueTest {

    @Test
    void value_preservesBigDecimal() {
        BigDecimal bd = new BigDecimal("123456789012345678901234567890");
        JsonDecimalValue v = new JsonDecimalValue(bd);
        assertSame(bd, v.value());
    }

    @Test
    void numberValue_returnsTheBigDecimal() {
        BigDecimal bd = new BigDecimal("0.0001");
        JsonDecimalValue v = new JsonDecimalValue(bd);
        assertSame(bd, v.numberValue());
    }

    @Test
    void scaleIsPreserved() {
        BigDecimal bd = new BigDecimal("1.2300");
        JsonDecimalValue v = new JsonDecimalValue(bd);
        assertEquals(4, ((BigDecimal) v.numberValue()).scale());
    }

    @Test
    void typeFlags_onlyIsNumber() {
        JsonDecimalValue v = new JsonDecimalValue(BigDecimal.ZERO);
        assertTrue(v.isNumber());
        assertFalse(v.isObject());
    }
}
