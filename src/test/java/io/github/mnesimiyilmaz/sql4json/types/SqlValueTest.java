package io.github.mnesimiyilmaz.sql4json.types;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class SqlValueTest {

    // SqlNull
    @Test
    void sqlNull_isSingleton() {
        assertSame(SqlNull.INSTANCE, SqlNull.INSTANCE);
    }

    @Test
    void sqlNull_isNull() {
        assertTrue(SqlNull.INSTANCE.isNull());
        assertNull(SqlNull.INSTANCE.rawValue());
    }

    // SqlBoolean
    @Test
    void sqlBoolean_constants_areSingletons() {
        assertSame(SqlBoolean.TRUE, SqlBoolean.of(true));
        assertSame(SqlBoolean.FALSE, SqlBoolean.of(false));
    }

    @Test
    void sqlBoolean_rawValue() {
        assertEquals(true, SqlBoolean.TRUE.rawValue());
        assertEquals(false, SqlBoolean.FALSE.rawValue());
    }

    @Test
    void sqlBoolean_isNotNull() {
        assertFalse(SqlBoolean.TRUE.isNull());
    }

    // SqlNumber
    @Test
    void sqlNumber_flyweight_smallIntegers() {
        assertSame(SqlNumber.of(0L), SqlNumber.of(0L));
        assertSame(SqlNumber.of(255L), SqlNumber.of(255L));
        assertSame(SqlNumber.of(42L), SqlNumber.of(42L));
    }

    @Test
    void sqlNumber_flyweight_outOfRange_notCached() {
        assertNotSame(SqlNumber.of(256L), SqlNumber.of(256L));
        assertNotSame(SqlNumber.of(-1L), SqlNumber.of(-1L));
    }

    @Test
    void sqlNumber_doubleValue() {
        assertEquals(3.14, SqlNumber.of(3.14).doubleValue(), 1e-10);
    }

    @Test
    void sqlNumber_longValue() {
        assertEquals(100L, SqlNumber.of(100L).longValue());
    }

    @Test
    void sqlNumber_rawValue() {
        assertEquals(42L, SqlNumber.of(42L).rawValue());
    }

    @Test
    void sqlNumber_isNotNull() {
        assertFalse(SqlNumber.of(1L).isNull());
    }

    // SqlString
    @Test
    void sqlString_value() {
        SqlString s = new SqlString("hello");
        assertEquals("hello", s.value());
        assertEquals("hello", s.rawValue());
    }

    @Test
    void sqlString_isNotNull() {
        assertFalse(new SqlString("x").isNull());
    }

    // SqlDate
    @Test
    void sqlDate_value() {
        LocalDate d = LocalDate.of(2024, 1, 15);
        SqlDate sd = new SqlDate(d);
        assertEquals(d, sd.value());
        assertEquals(d, sd.rawValue());
    }

    // SqlDateTime
    @Test
    void sqlDateTime_value() {
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30);
        SqlDateTime sdt = new SqlDateTime(dt);
        assertEquals(dt, sdt.value());
        assertEquals(dt, sdt.rawValue());
    }

    // Pattern matching exhaustiveness (compile-time check)
    @Test
    void patternMatching_exhaustive() {
        SqlValue v = SqlNumber.of(1L);
        String result = switch (v) {
            case SqlNumber n -> "number";
            case SqlString s -> "string";
            case SqlBoolean b -> "boolean";
            case SqlDate d -> "date";
            case SqlDateTime dt -> "datetime";
            case SqlNull ignored -> "null";
        };
        assertEquals("number", result);
    }

    // ── SqlNumber cache edge cases ─────────────────────────────────────────

    @Test
    void sqlNumber_of_Number_integerInCacheRange() {
        // SqlNumber.of(Number) with Integer in cache range
        Number n = Integer.valueOf(42);
        SqlNumber cached = SqlNumber.of(n);
        assertSame(SqlNumber.of(42L), cached);
    }

    @Test
    void sqlNumber_of_Number_integerOutOfRange() {
        // SqlNumber.of(Number) with Integer out of cache range
        Number n = Integer.valueOf(300);
        SqlNumber result = SqlNumber.of(n);
        assertEquals(300L, result.longValue());
    }

    @Test
    void sqlNumber_of_Number_longInCacheRange() {
        // SqlNumber.of(Number) with Long in cache range
        Number n = Long.valueOf(100);
        SqlNumber cached = SqlNumber.of(n);
        assertSame(SqlNumber.of(100L), cached);
    }

    @Test
    void sqlNumber_of_Number_longOutOfRange() {
        // SqlNumber.of(Number) with Long out of cache range
        Number n = Long.valueOf(1000);
        SqlNumber result = SqlNumber.of(n);
        assertEquals(1000L, result.longValue());
    }

    @Test
    void sqlNumber_of_Number_negative() {
        // SqlNumber.of(Number) with negative value → not cached
        Number n = Integer.valueOf(-1);
        SqlNumber result = SqlNumber.of(n);
        assertEquals(-1L, result.longValue());
    }

    @Test
    void sqlNumber_of_Number_double() {
        // SqlNumber.of(Number) with Double → not cached
        Number n = Double.valueOf(3.14);
        SqlNumber result = SqlNumber.of(n);
        assertEquals(3.14, result.doubleValue(), 1e-10);
    }

    @Test
    void sqlNumber_of_long_negative() {
        SqlNumber result = SqlNumber.of(-5L);
        assertEquals(-5L, result.longValue());
    }

    @Test
    void sqlNumber_of_long() {
        SqlNumber result = SqlNumber.of(123456789L);
        assertEquals(123456789L, result.longValue());
    }

    @Test
    void sqlNumber_sealedExhaustive() {
        SqlValue[] vals = {
                SqlNumber.of(1L),
                SqlNumber.of(1.5),
                SqlNumber.of(new java.math.BigDecimal("1.0001"))
        };
        for (SqlValue v : vals) {
            String tag = switch (v) {
                case SqlLong ignored    -> "long";
                case SqlDouble ignored  -> "double";
                case SqlDecimal ignored -> "decimal";
                default                 -> "other";
            };
            assertNotEquals("other", tag);
        }
    }
}
