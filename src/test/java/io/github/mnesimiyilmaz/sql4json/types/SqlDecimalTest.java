package io.github.mnesimiyilmaz.sql4json.types;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class SqlDecimalTest {

    @Test
    void of_bigDecimal_preservesValue() {
        BigDecimal bd = new BigDecimal("123456789012345678901234567890");
        SqlDecimal d = (SqlDecimal) SqlNumber.of(bd);
        assertSame(bd, d.value());
        assertSame(bd, d.bigDecimalValue());
    }

    @Test
    void scaleIsPreserved() {
        BigDecimal bd = new BigDecimal("1.2300");
        SqlDecimal d = (SqlDecimal) SqlNumber.of(bd);
        assertEquals(4, ((BigDecimal) d.numberValue()).scale());
    }

    @Test
    void numberFactoryRoutesToDecimalForBigDecimal() {
        Number n = new BigDecimal("0.001");
        assertInstanceOf(SqlDecimal.class, SqlNumber.of(n));
    }
}
