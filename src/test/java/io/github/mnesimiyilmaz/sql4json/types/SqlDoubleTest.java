// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.types;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class SqlDoubleTest {

    @Test
    void of_double_freshInstance() {
        SqlNumber a = SqlNumber.of(3.14);
        SqlNumber b = SqlNumber.of(3.14);
        assertEquals(a, b);
    }

    @Test
    void unboxedAccessors() {
        SqlDouble d = (SqlDouble) SqlNumber.of(2.5);
        assertEquals(2L, d.longValue());
        assertEquals(2.5, d.doubleValue());
        assertEquals(BigDecimal.valueOf(2.5), d.bigDecimalValue());
        assertEquals(Double.valueOf(2.5), d.numberValue());
        assertEquals(2.5, d.rawValue());
    }
}
