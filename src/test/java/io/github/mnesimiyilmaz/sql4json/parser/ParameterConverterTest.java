package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import io.github.mnesimiyilmaz.sql4json.types.*;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;

import static org.junit.jupiter.api.Assertions.*;

class ParameterConverterTest {

    @Test
    void when_string_then_SqlString() {
        assertEquals(new SqlString("x"), ParameterConverter.toSqlValue("x"));
    }

    @Test
    void when_integer_then_SqlNumber() {
        SqlValue v = ParameterConverter.toSqlValue(42);
        assertInstanceOf(SqlNumber.class, v);
    }

    @Test
    void when_long_then_SqlNumber() {
        SqlValue v = ParameterConverter.toSqlValue(42L);
        assertInstanceOf(SqlNumber.class, v);
    }

    @Test
    void when_double_then_SqlNumber() {
        SqlValue v = ParameterConverter.toSqlValue(3.14);
        assertInstanceOf(SqlNumber.class, v);
    }

    @Test
    void when_big_decimal_then_SqlNumber() {
        SqlValue v = ParameterConverter.toSqlValue(new BigDecimal("3.14"));
        assertInstanceOf(SqlNumber.class, v);
    }

    @Test
    void when_big_integer_then_SqlNumber() {
        SqlValue v = ParameterConverter.toSqlValue(new BigInteger("1000"));
        assertInstanceOf(SqlNumber.class, v);
    }

    @Test
    void when_boolean_then_SqlBoolean() {
        assertEquals(SqlBoolean.of(true), ParameterConverter.toSqlValue(true));
    }

    @Test
    void when_local_date_then_SqlDate() {
        SqlValue v = ParameterConverter.toSqlValue(LocalDate.of(2026, 4, 21));
        assertInstanceOf(SqlDate.class, v);
    }

    @Test
    void when_local_date_time_then_SqlDateTime() {
        SqlValue v = ParameterConverter.toSqlValue(LocalDateTime.of(2026, 4, 21, 10, 15));
        assertInstanceOf(SqlDateTime.class, v);
    }

    @Test
    void when_instant_then_SqlDateTime_utc() {
        Instant i = Instant.parse("2026-04-21T10:15:30Z");
        SqlValue v = ParameterConverter.toSqlValue(i);
        assertInstanceOf(SqlDateTime.class, v);
    }

    @Test
    void when_zoned_date_time_then_SqlDateTime_utc() {
        ZonedDateTime z = ZonedDateTime.of(2026, 4, 21, 12, 0, 0, 0, ZoneId.of("Europe/Istanbul"));
        SqlValue v = ParameterConverter.toSqlValue(z);
        assertInstanceOf(SqlDateTime.class, v);
        // Istanbul UTC+3 → UTC 9:00
        assertEquals(LocalDateTime.of(2026, 4, 21, 9, 0), ((SqlDateTime) v).value());
    }

    @Test
    void when_offset_date_time_then_SqlDateTime_utc() {
        OffsetDateTime o = OffsetDateTime.of(2026, 4, 21, 12, 0, 0, 0, ZoneOffset.ofHours(3));
        SqlValue v = ParameterConverter.toSqlValue(o);
        assertInstanceOf(SqlDateTime.class, v);
        assertEquals(LocalDateTime.of(2026, 4, 21, 9, 0), ((SqlDateTime) v).value());
    }

    @Test
    void when_java_util_date_then_SqlDateTime_utc() {
        java.util.Date d = java.util.Date.from(Instant.parse("2026-04-21T10:15:30Z"));
        SqlValue v = ParameterConverter.toSqlValue(d);
        assertInstanceOf(SqlDateTime.class, v);
    }

    @Test
    void when_null_then_SqlNull() {
        assertSame(SqlNull.INSTANCE, ParameterConverter.toSqlValue(null));
    }

    @Test
    void when_unsupported_type_then_exception() {
        assertThrows(SQL4JsonBindException.class,
                () -> ParameterConverter.toSqlValue(new java.io.File("x")));
    }

    @Test
    void when_sql_value_passed_through_then_returned_as_is() {
        // SqlValues bound as parameters (e.g., re-bind of a result) should pass through.
        SqlValue already = new SqlString("x");
        assertSame(already, ParameterConverter.toSqlValue(already));
    }

    @Test
    void when_float_then_SqlNumber() {
        SqlValue v = ParameterConverter.toSqlValue(3.14f);
        assertInstanceOf(SqlNumber.class, v);
    }
}
