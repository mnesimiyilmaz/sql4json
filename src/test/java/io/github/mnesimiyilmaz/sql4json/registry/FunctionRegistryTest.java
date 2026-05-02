// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.types.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FunctionRegistryTest {

    private final FunctionRegistry registry = FunctionRegistry.createDefault();

    // ── Sealed SqlFunction pattern matching ──────────────────────────────────

    @Test
    void sqlFunction_patternMatch_exhaustive() {
        SqlFunction fn = new ScalarFunction("test", (v, args) -> v);
        String type =
                switch (fn) {
                    case ScalarFunction s -> "scalar";
                    case ValueFunction v -> "value";
                    case AggregateFunction a -> "aggregate";
                };
        assertEquals("scalar", type);
    }

    // ── Scalar: LOWER ────────────────────────────────────────────────────────

    @Test
    void lower_string_toLowercase() {
        var fn = registry.getScalar("lower").orElseThrow();
        SqlValue result = fn.apply().apply(new SqlString("HELLO"), List.of());
        assertEquals(new SqlString("hello"), result);
    }

    @Test
    void lower_nonString_coerced() {
        // Since 1.2.0 LOWER coerces non-string inputs via toString → "42"
        var fn = registry.getScalar("lower").orElseThrow();
        SqlValue result = fn.apply().apply(SqlNumber.of(42), List.of());
        assertEquals(new SqlString("42"), result);
    }

    // ── Scalar: UPPER ────────────────────────────────────────────────────────

    @Test
    void upper_string_toUppercase() {
        var fn = registry.getScalar("upper").orElseThrow();
        SqlValue result = fn.apply().apply(new SqlString("hello"), List.of());
        assertEquals(new SqlString("HELLO"), result);
    }

    // ── Scalar: COALESCE ─────────────────────────────────────────────────────

    @Test
    void coalesce_nullFirst_returnsFirstNonNull() {
        var fn = registry.getScalar("coalesce").orElseThrow();
        SqlValue result = fn.apply().apply(SqlNull.INSTANCE, List.of(new SqlString("fallback")));
        assertEquals(new SqlString("fallback"), result);
    }

    @Test
    void coalesce_nonNullFirst_returnsIt() {
        var fn = registry.getScalar("coalesce").orElseThrow();
        SqlValue result = fn.apply().apply(new SqlString("value"), List.of(new SqlString("fallback")));
        assertEquals(new SqlString("value"), result);
    }

    @Test
    void coalesce_allNull_returnsNull() {
        var fn = registry.getScalar("coalesce").orElseThrow();
        SqlValue result = fn.apply().apply(SqlNull.INSTANCE, List.of(SqlNull.INSTANCE));
        assertSame(SqlNull.INSTANCE, result);
    }

    // ── Scalar: TO_DATE ──────────────────────────────────────────────────────

    @Test
    void toDate_defaultFormat_returnsDate() {
        var fn = registry.getScalar("to_date").orElseThrow();
        SqlValue result = fn.apply().apply(new SqlString("2024-06-15"), List.of());
        assertInstanceOf(SqlDate.class, result);
        assertEquals(LocalDate.of(2024, 6, 15), ((SqlDate) result).value());
    }

    @Test
    void toDate_customFormat_returnsDate() {
        var fn = registry.getScalar("to_date").orElseThrow();
        SqlValue result = fn.apply().apply(new SqlString("15/06/2024"), List.of(new SqlString("dd/MM/yyyy")));
        assertInstanceOf(SqlDate.class, result);
        assertEquals(LocalDate.of(2024, 6, 15), ((SqlDate) result).value());
    }

    @Test
    void toDate_dateInput_passesThrough() {
        // Since 1.2.0 TO_DATE preserves only date/datetime types unchanged; other types
        // are coerced and parsed. SqlDate input short-circuits.
        var fn = registry.getScalar("to_date").orElseThrow();
        SqlDate input = new SqlDate(LocalDate.of(2024, 6, 15));
        SqlValue result = fn.apply().apply(input, List.of());
        assertEquals(input, result);
    }

    // ── Value: NOW ───────────────────────────────────────────────────────────

    @Test
    void now_returnsDateTime() {
        var fn = registry.getValue("now").orElseThrow();
        SqlValue result = fn.apply().get();
        assertInstanceOf(SqlDateTime.class, result);
    }

    // ── Aggregate: COUNT ─────────────────────────────────────────────────────

    @Test
    void count_returnsSize() {
        var fn = registry.getAggregate("count").orElseThrow();
        SqlValue result = fn.apply().apply(List.of(new SqlString("a"), new SqlString("b"), new SqlString("c")));
        assertInstanceOf(SqlNumber.class, result);
        assertEquals(3.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void count_emptyList_returnsZero() {
        var fn = registry.getAggregate("count").orElseThrow();
        SqlValue result = fn.apply().apply(List.of());
        assertEquals(0.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    // ── Aggregate: SUM ───────────────────────────────────────────────────────

    @Test
    void sum_numbers() {
        var fn = registry.getAggregate("sum").orElseThrow();
        SqlValue result = fn.apply().apply(List.of(SqlNumber.of(1), SqlNumber.of(2), SqlNumber.of(3)));
        assertEquals(6.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void sum_nonNumbersIgnored() {
        var fn = registry.getAggregate("sum").orElseThrow();
        SqlValue result = fn.apply().apply(List.of(SqlNumber.of(5), new SqlString("x"), SqlNumber.of(5)));
        assertEquals(10.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    // ── Aggregate: AVG ───────────────────────────────────────────────────────

    @Test
    void avg_numbers() {
        var fn = registry.getAggregate("avg").orElseThrow();
        SqlValue result = fn.apply().apply(List.of(SqlNumber.of(10), SqlNumber.of(20), SqlNumber.of(30)));
        assertEquals(20.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void avg_emptyList_returnsSqlNull() {
        // BUG FIX: AVG of empty list must not throw ArithmeticException
        var fn = registry.getAggregate("avg").orElseThrow();
        SqlValue result = fn.apply().apply(List.of());
        assertSame(SqlNull.INSTANCE, result, "AVG of empty list must return SqlNull, not throw");
    }

    // ── Aggregate: MIN ───────────────────────────────────────────────────────

    @Test
    void min_numbers() {
        var fn = registry.getAggregate("min").orElseThrow();
        SqlValue result = fn.apply().apply(List.of(SqlNumber.of(3), SqlNumber.of(1), SqlNumber.of(2)));
        assertEquals(1.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void min_nullsIgnored() {
        var fn = registry.getAggregate("min").orElseThrow();
        SqlValue result = fn.apply().apply(List.of(SqlNull.INSTANCE, SqlNumber.of(5), SqlNull.INSTANCE));
        assertEquals(5.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void min_allNulls_returnsSqlNull() {
        var fn = registry.getAggregate("min").orElseThrow();
        SqlValue result = fn.apply().apply(List.of(SqlNull.INSTANCE));
        assertSame(SqlNull.INSTANCE, result);
    }

    // ── Aggregate: MAX ───────────────────────────────────────────────────────

    @Test
    void max_numbers() {
        var fn = registry.getAggregate("max").orElseThrow();
        SqlValue result = fn.apply().apply(List.of(SqlNumber.of(1), SqlNumber.of(3), SqlNumber.of(2)));
        assertEquals(3.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void max_strings() {
        var fn = registry.getAggregate("max").orElseThrow();
        SqlValue result =
                fn.apply().apply(List.of(new SqlString("apple"), new SqlString("zebra"), new SqlString("mango")));
        assertInstanceOf(SqlString.class, result);
        assertEquals("zebra", ((SqlString) result).value());
    }

    // ── Case-insensitive lookup ───────────────────────────────────────────────

    @Test
    void lookup_caseInsensitive_scalar() {
        assertTrue(registry.getScalar("LOWER").isPresent());
        assertTrue(registry.getScalar("Lower").isPresent());
        assertTrue(registry.getScalar("lower").isPresent());
    }

    @Test
    void lookup_caseInsensitive_aggregate() {
        assertTrue(registry.getAggregate("COUNT").isPresent());
        assertTrue(registry.getAggregate("Count").isPresent());
    }

    @Test
    void lookup_unknown_scalar_returnsEmpty() {
        assertTrue(registry.getScalar("nonexistent").isEmpty());
    }

    @Test
    void lookup_unknown_aggregate_returnsEmpty() {
        assertTrue(registry.getAggregate("nonexistent").isEmpty());
    }

    // ── Date functions on SqlDate (not SqlDateTime) ─────────────────────

    @ParameterizedTest
    @ValueSource(strings = {"hour", "minute", "second"})
    void timeComponent_onDate_returnsZero(String fnName) {
        var fn = registry.getScalar(fnName).orElseThrow();
        SqlValue result = fn.apply().apply(new SqlDate(LocalDate.of(2024, 6, 15)), List.of());
        assertEquals(SqlNumber.of(0), result);
    }

    @ParameterizedTest
    @CsvSource({"hour,14", "minute,30", "second,45"})
    void timeComponent_onDateTime_returnsExpected(String fnName, int expected) {
        var fn = registry.getScalar(fnName).orElseThrow();
        SqlValue result = fn.apply().apply(new SqlDateTime(LocalDateTime.of(2024, 6, 15, 14, 30, 45)), List.of());
        assertEquals(SqlNumber.of(expected), result);
    }

    @Test
    void hour_onNull_returnsNull() {
        var fn = registry.getScalar("hour").orElseThrow();
        SqlValue result = fn.apply().apply(SqlNull.INSTANCE, List.of());
        assertSame(SqlNull.INSTANCE, result);
    }

    // ── DATE_ADD edge cases ─────────────────────────────────────────────

    @Test
    void dateAdd_hoursOnDate_promotesToDateTime() {
        var fn = registry.getScalar("date_add").orElseThrow();
        SqlValue result = fn.apply()
                .apply(new SqlDate(LocalDate.of(2024, 1, 1)), List.of(SqlNumber.of(5), new SqlString("HOUR")));
        assertInstanceOf(SqlDateTime.class, result);
        assertEquals(LocalDateTime.of(2024, 1, 1, 5, 0), ((SqlDateTime) result).value());
    }

    @Test
    void dateAdd_daysOnDate_staysDate() {
        var fn = registry.getScalar("date_add").orElseThrow();
        SqlValue result = fn.apply()
                .apply(new SqlDate(LocalDate.of(2024, 1, 1)), List.of(SqlNumber.of(10), new SqlString("DAY")));
        assertInstanceOf(SqlDate.class, result);
        assertEquals(LocalDate.of(2024, 1, 11), ((SqlDate) result).value());
    }

    @Test
    void dateAdd_onNull_returnsNull() {
        var fn = registry.getScalar("date_add").orElseThrow();
        SqlValue result = fn.apply().apply(SqlNull.INSTANCE, List.of(SqlNumber.of(1), new SqlString("DAY")));
        assertSame(SqlNull.INSTANCE, result);
    }

    @Test
    void dateAdd_minutesOnDate_promotesToDateTime() {
        var fn = registry.getScalar("date_add").orElseThrow();
        SqlValue result = fn.apply()
                .apply(new SqlDate(LocalDate.of(2024, 6, 1)), List.of(SqlNumber.of(90), new SqlString("MINUTE")));
        assertInstanceOf(SqlDateTime.class, result);
    }

    @Test
    void dateAdd_secondsOnDate_promotesToDateTime() {
        var fn = registry.getScalar("date_add").orElseThrow();
        SqlValue result = fn.apply()
                .apply(new SqlDate(LocalDate.of(2024, 6, 1)), List.of(SqlNumber.of(3600), new SqlString("SECOND")));
        assertInstanceOf(SqlDateTime.class, result);
    }

    // ── DATE_DIFF edge cases ────────────────────────────────────────────

    @Test
    void dateDiff_betweenDates_inDays() {
        var fn = registry.getScalar("date_diff").orElseThrow();
        SqlValue result = fn.apply()
                .apply(
                        new SqlDate(LocalDate.of(2024, 1, 11)),
                        List.of(new SqlDate(LocalDate.of(2024, 1, 1)), new SqlString("DAY")));
        assertEquals(SqlNumber.of(10), result);
    }

    @Test
    void dateDiff_dateAndDateTime_mixed() {
        var fn = registry.getScalar("date_diff").orElseThrow();
        SqlValue result = fn.apply()
                .apply(
                        new SqlDateTime(LocalDateTime.of(2024, 1, 2, 12, 0)),
                        List.of(new SqlDate(LocalDate.of(2024, 1, 1)), new SqlString("HOUR")));
        assertEquals(SqlNumber.of(36), result);
    }

    @Test
    void dateDiff_onNull_returnsNull() {
        var fn = registry.getScalar("date_diff").orElseThrow();
        SqlValue result = fn.apply()
                .apply(SqlNull.INSTANCE, List.of(new SqlDate(LocalDate.of(2024, 1, 1)), new SqlString("DAY")));
        assertSame(SqlNull.INSTANCE, result);
    }

    @Test
    void dateDiff_nullSecondArg_returnsNull() {
        var fn = registry.getScalar("date_diff").orElseThrow();
        SqlValue result = fn.apply()
                .apply(new SqlDate(LocalDate.of(2024, 1, 1)), List.of(SqlNull.INSTANCE, new SqlString("DAY")));
        assertSame(SqlNull.INSTANCE, result);
    }

    // ── Date-helper JDK exception wrapping ──────────────────────────────

    @Test
    void dateAdd_badUnit_throwsExecutionException() {
        var fn = registry.getScalar("date_add").orElseThrow().apply();
        SqlDate date = new SqlDate(LocalDate.of(2024, 1, 1));
        var ex = assertThrows(
                SQL4JsonExecutionException.class, () -> fn.apply(date, List.of(SqlNumber.of(1), new SqlString("FOO"))));
        assertTrue(
                ex.getMessage().contains("Unsupported date unit"),
                "Expected message to contain 'Unsupported date unit' but was: " + ex.getMessage());
        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
    }

    @Test
    void dateDiff_badUnit_throwsExecutionException() {
        var fn = registry.getScalar("date_diff").orElseThrow().apply();
        SqlDate date = new SqlDate(LocalDate.of(2024, 1, 11));
        SqlDate date2 = new SqlDate(LocalDate.of(2024, 1, 1));
        var ex = assertThrows(
                SQL4JsonExecutionException.class, () -> fn.apply(date, List.of(date2, new SqlString("FOO"))));
        assertTrue(
                ex.getMessage().contains("Unsupported date unit"),
                "Expected message to contain 'Unsupported date unit' but was: " + ex.getMessage());
        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
    }

    @Test
    void toDate_badFormatPattern_throwsExecutionException() {
        // 'p' is a reserved pad-letter that must be followed by a valid pad pattern —
        // DateTimeFormatter.ofPattern("p") always throws IllegalArgumentException.
        var fn = registry.getScalar("to_date").orElseThrow().apply();
        var ex = assertThrows(
                SQL4JsonExecutionException.class,
                () -> fn.apply(new SqlString("2024-01-01"), List.of(new SqlString("p"))));
        assertTrue(
                ex.getMessage().contains("Invalid date format pattern"),
                "Expected message to contain 'Invalid date format pattern' but was: " + ex.getMessage());
        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
    }

    // ── CAST edge cases ─────────────────────────────────────────────────

    @Test
    void cast_booleanToNumber() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlValue result = fn.apply().apply(SqlBoolean.TRUE, List.of(new SqlString("NUMBER")));
        assertInstanceOf(SqlNumber.class, result);
        assertEquals(1.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void cast_booleanToInteger() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlValue result = fn.apply().apply(SqlBoolean.TRUE, List.of(new SqlString("INTEGER")));
        assertInstanceOf(SqlNumber.class, result);
        assertEquals(1.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void cast_dateToNumber_epochDay() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlDate d = new SqlDate(LocalDate.of(2024, 1, 1));
        SqlValue result = fn.apply().apply(d, List.of(new SqlString("NUMBER")));
        assertInstanceOf(SqlNumber.class, result);
        assertEquals((double) LocalDate.of(2024, 1, 1).toEpochDay(), ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void cast_dateTimeToNumber_epochSeconds() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlDateTime dt = new SqlDateTime(LocalDateTime.of(2024, 1, 1, 0, 0));
        SqlValue result = fn.apply().apply(dt, List.of(new SqlString("NUMBER")));
        assertInstanceOf(SqlNumber.class, result);
    }

    @Test
    void cast_dateTimeToDate() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlDateTime dt = new SqlDateTime(LocalDateTime.of(2024, 6, 15, 14, 30));
        SqlValue result = fn.apply().apply(dt, List.of(new SqlString("DATE")));
        assertInstanceOf(SqlDate.class, result);
        assertEquals(LocalDate.of(2024, 6, 15), ((SqlDate) result).value());
    }

    @Test
    void cast_dateToDateTime() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlDate d = new SqlDate(LocalDate.of(2024, 6, 15));
        SqlValue result = fn.apply().apply(d, List.of(new SqlString("DATETIME")));
        assertInstanceOf(SqlDateTime.class, result);
        assertEquals(LocalDateTime.of(2024, 6, 15, 0, 0), ((SqlDateTime) result).value());
    }

    @Test
    void cast_booleanToString() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlValue result = fn.apply().apply(SqlBoolean.TRUE, List.of(new SqlString("STRING")));
        assertEquals(new SqlString("true"), result);
    }

    @Test
    void cast_dateToString() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlDate d = new SqlDate(LocalDate.of(2024, 6, 15));
        SqlValue result = fn.apply().apply(d, List.of(new SqlString("STRING")));
        assertEquals(new SqlString("2024-06-15"), result);
    }

    @Test
    void cast_dateTimeToString() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlDateTime dt = new SqlDateTime(LocalDateTime.of(2024, 6, 15, 10, 30));
        SqlValue result = fn.apply().apply(dt, List.of(new SqlString("STRING")));
        assertEquals(new SqlString("2024-06-15T10:30"), result);
    }

    @Test
    void cast_dateToInteger_throws() {
        var function = registry.getScalar("cast").orElseThrow().apply();
        SqlDate d = new SqlDate(LocalDate.of(2024, 1, 1));
        assertThrows(SQL4JsonExecutionException.class, () -> function.apply(d, List.of(new SqlString("INTEGER"))));
    }

    @Test
    void cast_dateToBoolean_throws() {
        var function = registry.getScalar("cast").orElseThrow().apply();
        SqlDate d = new SqlDate(LocalDate.of(2024, 1, 1));
        assertThrows(SQL4JsonExecutionException.class, () -> function.apply(d, List.of(new SqlString("BOOLEAN"))));
    }

    @Test
    void cast_numberToDate_throws() {
        var function = registry.getScalar("cast").orElseThrow().apply();
        assertThrows(
                SQL4JsonExecutionException.class,
                () -> function.apply(SqlNumber.of(42), List.of(new SqlString("DATE"))));
    }

    @Test
    void cast_numberToDateTime_throws() {
        var function = registry.getScalar("cast").orElseThrow().apply();
        assertThrows(
                SQL4JsonExecutionException.class,
                () -> function.apply(SqlNumber.of(42), List.of(new SqlString("DATETIME"))));
    }

    @Test
    void cast_null_returnsNull() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlValue result = fn.apply().apply(SqlNull.INSTANCE, List.of(new SqlString("STRING")));
        assertSame(SqlNull.INSTANCE, result);
    }

    @Test
    void cast_unknownType_throws() {
        var function = registry.getScalar("cast").orElseThrow().apply();
        assertThrows(
                SQL4JsonExecutionException.class,
                () -> function.apply(new SqlString("hello"), List.of(new SqlString("BLOB"))));
    }

    @Test
    void cast_stringToInteger_decimal_truncates() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlValue result = fn.apply().apply(new SqlString("3.7"), List.of(new SqlString("INTEGER")));
        assertInstanceOf(SqlNumber.class, result);
        assertEquals(3.0, ((SqlNumber) result).doubleValue(), 1e-10);
    }

    @Test
    void cast_stringToBoolean() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlValue result = fn.apply().apply(new SqlString("true"), List.of(new SqlString("BOOLEAN")));
        assertEquals(SqlBoolean.TRUE, result);
    }

    @Test
    void cast_numberToBoolean_nonZeroIsTrue() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlValue result = fn.apply().apply(SqlNumber.of(42), List.of(new SqlString("BOOLEAN")));
        assertEquals(SqlBoolean.TRUE, result);
    }

    @Test
    void cast_numberToBoolean_zeroIsFalse() {
        var fn = registry.getScalar("cast").orElseThrow();
        SqlValue result = fn.apply().apply(SqlNumber.of(0), List.of(new SqlString("BOOLEAN")));
        assertEquals(SqlBoolean.FALSE, result);
    }

    // ── Enumeration methods ──────────────────────────────────────────────

    @Test
    void scalarFunctionNamesContainsKnownEntries() {
        var names = FunctionRegistry.getDefault().scalarFunctionNames();
        assertTrue(names.contains("lower"));
        assertTrue(names.contains("abs"));
        assertTrue(names.contains("cast"));
    }

    @Test
    void valueFunctionNamesContainsNow() {
        var names = FunctionRegistry.getDefault().valueFunctionNames();
        assertTrue(names.contains("now"));
    }

    @Test
    void aggregateFunctionNamesContainsAllFive() {
        var names = FunctionRegistry.getDefault().aggregateFunctionNames();
        assertTrue(names.containsAll(java.util.List.of("count", "sum", "avg", "min", "max")));
    }
}
