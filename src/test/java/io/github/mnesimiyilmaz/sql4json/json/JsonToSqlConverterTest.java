// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.types.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonToSqlConverterTest {

    // JsonValue → SqlValue
    @Test
    void string_toSqlString() {
        SqlValue result = JsonToSqlConverter.toSqlValue(new JsonStringValue("hello"));
        assertInstanceOf(SqlString.class, result);
        assertEquals("hello", ((SqlString) result).value());
    }

    @Test
    void number_toSqlNumber() {
        SqlValue result = JsonToSqlConverter.toSqlValue(new JsonLongValue(42L));
        assertInstanceOf(SqlNumber.class, result);
        assertEquals(42.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void boolean_toSqlBoolean() {
        SqlValue result = JsonToSqlConverter.toSqlValue(new JsonBooleanValue(true));
        assertSame(SqlBoolean.TRUE, result);
    }

    @Test
    void null_toSqlNull() {
        SqlValue result = JsonToSqlConverter.toSqlValue(JsonNullValue.INSTANCE);
        assertSame(SqlNull.INSTANCE, result);
    }

    @Test
    void object_throwsException() {
        JsonObjectValue input = new JsonObjectValue(Map.of());
        assertThrows(SQL4JsonExecutionException.class, () -> JsonToSqlConverter.toSqlValue(input));
    }

    @Test
    void array_throwsException() {
        JsonArrayValue input = new JsonArrayValue(List.of());
        assertThrows(SQL4JsonExecutionException.class, () -> JsonToSqlConverter.toSqlValue(input));
    }

    // SqlValue → JsonValue
    @Test
    void sqlString_toJsonString() {
        JsonValue result = JsonToSqlConverter.toJsonValue(new SqlString("world"));
        assertInstanceOf(JsonStringValue.class, result);
        assertEquals("world", ((JsonStringValue) result).value());
    }

    @Test
    void sqlNumber_toJsonNumber() {
        JsonValue result = JsonToSqlConverter.toJsonValue(SqlNumber.of(7));
        assertInstanceOf(JsonNumberValue.class, result);
        assertEquals(7, ((JsonNumberValue) result).numberValue().intValue());
    }

    @Test
    void sqlBoolean_toJsonBoolean() {
        JsonValue result = JsonToSqlConverter.toJsonValue(SqlBoolean.TRUE);
        assertInstanceOf(JsonBooleanValue.class, result);
        assertTrue(((JsonBooleanValue) result).value());
    }

    @Test
    void sqlNull_toJsonNull() {
        JsonValue result = JsonToSqlConverter.toJsonValue(SqlNull.INSTANCE);
        assertSame(JsonNullValue.INSTANCE, result);
    }

    @Test
    void sqlDate_toJsonString_isoFormat() {
        SqlDate d = new SqlDate(java.time.LocalDate.of(2024, 6, 15));
        JsonValue result = JsonToSqlConverter.toJsonValue(d);
        assertInstanceOf(JsonStringValue.class, result);
        assertEquals("2024-06-15", ((JsonStringValue) result).value());
    }

    @Test
    void sqlDateTime_toJsonString_isoFormat() {
        SqlDateTime dt = new SqlDateTime(java.time.LocalDateTime.of(2024, 6, 15, 10, 30, 0));
        JsonValue result = JsonToSqlConverter.toJsonValue(dt);
        assertInstanceOf(JsonStringValue.class, result);
        assertEquals("2024-06-15T10:30", ((JsonStringValue) result).value());
    }

    // toSqlValueSafe: objects/arrays → SqlNull instead of throwing
    @Test
    void safe_object_returnsSqlNull() {
        SqlValue result = JsonToSqlConverter.toSqlValueSafe(new JsonObjectValue(Map.of()));
        assertSame(SqlNull.INSTANCE, result);
    }

    @Test
    void safe_array_returnsSqlNull() {
        SqlValue result = JsonToSqlConverter.toSqlValueSafe(new JsonArrayValue(List.of()));
        assertSame(SqlNull.INSTANCE, result);
    }

    @Test
    void safe_string_returnsSqlString() {
        SqlValue result = JsonToSqlConverter.toSqlValueSafe(new JsonStringValue("hi"));
        assertInstanceOf(SqlString.class, result);
        assertEquals("hi", ((SqlString) result).value());
    }

    @Test
    void safe_null_returnsSqlNull() {
        SqlValue result = JsonToSqlConverter.toSqlValueSafe(JsonNullValue.INSTANCE);
        assertSame(SqlNull.INSTANCE, result);
    }

    // Round-trip: SqlValue → JsonValue → SqlValue
    @Test
    void roundTrip_string() {
        SqlString original = new SqlString("test");
        SqlValue back = JsonToSqlConverter.toSqlValue(JsonToSqlConverter.toJsonValue(original));
        assertEquals(original, back);
    }

    @Test
    void roundTrip_number() {
        SqlNumber original = SqlNumber.of(3.14);
        SqlValue back = JsonToSqlConverter.toSqlValue(JsonToSqlConverter.toJsonValue(original));
        assertInstanceOf(SqlNumber.class, back);
        assertEquals(3.14, ((SqlNumber) back).doubleValue(), 1e-10);
    }
}
