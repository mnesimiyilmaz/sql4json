package io.github.mnesimiyilmaz.sql4json.types;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class SqlLongTest {

    @Test
    void of_long_smallCache() {
        assertSame(SqlNumber.of(0L), SqlNumber.of(0L));
        assertSame(SqlNumber.of(255L), SqlNumber.of(255L));
        assertSame(SqlNumber.of(42L), SqlNumber.of(42L));
    }

    @Test
    void of_long_outOfRange_freshInstance() {
        assertNotSame(SqlNumber.of(-1L), SqlNumber.of(-1L));
        assertNotSame(SqlNumber.of(256L), SqlNumber.of(256L));
    }

    @Test
    void unboxedAccessors() {
        SqlLong l = (SqlLong) SqlNumber.of(99L);
        assertEquals(99L, l.longValue());
        assertEquals(99.0, l.doubleValue(), 0.0);
        assertEquals(BigDecimal.valueOf(99L), l.bigDecimalValue());
        assertEquals(Long.valueOf(99L), l.numberValue());
        assertEquals(99L, l.rawValue());
    }

    @Test
    void implementsSqlNumberAndSqlValue() {
        SqlLong l = new SqlLong(0L);
        assertInstanceOf(SqlNumber.class, l);
        assertInstanceOf(SqlValue.class, l);
        assertFalse(l.isNull());
    }
}
