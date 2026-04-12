package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.SQL4Json;
import io.github.mnesimiyilmaz.sql4json.types.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class DateCoercionTest {

    // ── toLocalDate ──────────────────────────────────────────────────────────

    @Test
    void toLocalDate_fromSqlDate_returnsValue() {
        LocalDate d = LocalDate.of(2021, 8, 19);
        assertEquals(d, DateCoercion.toLocalDate(new SqlDate(d)));
    }

    @Test
    void toLocalDate_fromSqlDateTime_returnsDatePart() {
        LocalDateTime dt = LocalDateTime.of(2021, 8, 19, 10, 30, 0);
        assertEquals(LocalDate.of(2021, 8, 19), DateCoercion.toLocalDate(new SqlDateTime(dt)));
    }

    @Test
    void toLocalDate_fromIsoDateString_parsesCorrectly() {
        assertEquals(LocalDate.of(2021, 8, 19),
                DateCoercion.toLocalDate(new SqlString("2021-08-19")));
    }

    @Test
    void toLocalDate_fromIsoDateTimeString_returnsDatePart() {
        assertEquals(LocalDate.of(2021, 8, 19),
                DateCoercion.toLocalDate(new SqlString("2021-08-19T10:30:00")));
    }

    @Test
    void toLocalDate_fromOffsetDateTimeString_returnsDatePart() {
        assertEquals(LocalDate.of(2021, 8, 19),
                DateCoercion.toLocalDate(new SqlString("2021-08-19T10:30:00+03:00")));
    }

    @Test
    void toLocalDate_fromZuluString_returnsDatePart() {
        assertEquals(LocalDate.of(2021, 8, 19),
                DateCoercion.toLocalDate(new SqlString("2021-08-19T10:30:00Z")));
    }

    @Test
    void toLocalDate_fromInvalidString_returnsNull() {
        assertNull(DateCoercion.toLocalDate(new SqlString("not-a-date")));
    }

    @Test
    void toLocalDate_fromNull_returnsNull() {
        assertNull(DateCoercion.toLocalDate(SqlNull.INSTANCE));
    }

    @Test
    void toLocalDate_fromNumber_returnsNull() {
        assertNull(DateCoercion.toLocalDate(SqlNumber.of(42)));
    }

    // ── toLocalDateTime ──────────────────────────────────────────────────────

    @Test
    void toLocalDateTime_fromSqlDateTime_returnsValue() {
        LocalDateTime dt = LocalDateTime.of(2021, 8, 19, 10, 30, 0);
        assertEquals(dt, DateCoercion.toLocalDateTime(new SqlDateTime(dt)));
    }

    @Test
    void toLocalDateTime_fromSqlDate_returnsStartOfDay() {
        LocalDate d = LocalDate.of(2021, 8, 19);
        assertEquals(d.atStartOfDay(), DateCoercion.toLocalDateTime(new SqlDate(d)));
    }

    @Test
    void toLocalDateTime_fromIsoDateString_returnsStartOfDay() {
        assertEquals(LocalDate.of(2021, 8, 19).atStartOfDay(),
                DateCoercion.toLocalDateTime(new SqlString("2021-08-19")));
    }

    @Test
    void toLocalDateTime_fromIsoDateTimeString_parsesCorrectly() {
        assertEquals(LocalDateTime.of(2021, 8, 19, 10, 30, 0),
                DateCoercion.toLocalDateTime(new SqlString("2021-08-19T10:30:00")));
    }

    @Test
    void toLocalDateTime_fromOffsetDateTimeString_stripsOffset() {
        // Offset-aware string: local part is 10:30:00 regardless of offset
        LocalDateTime result = DateCoercion.toLocalDateTime(new SqlString("2021-08-19T10:30:00+03:00"));
        assertNotNull(result);
        assertEquals(LocalDateTime.of(2021, 8, 19, 10, 30, 0), result);
    }

    @Test
    void toLocalDateTime_fromZuluString_stripsOffset() {
        LocalDateTime result = DateCoercion.toLocalDateTime(new SqlString("2021-08-19T10:30:00Z"));
        assertNotNull(result);
        assertEquals(LocalDateTime.of(2021, 8, 19, 10, 30, 0), result);
    }

    @Test
    void toLocalDateTime_fromInvalidString_returnsNull() {
        assertNull(DateCoercion.toLocalDateTime(new SqlString("not-a-datetime")));
    }

    @Test
    void toLocalDateTime_fromNumber_returnsNull() {
        assertNull(DateCoercion.toLocalDateTime(SqlNumber.of(42)));
    }

    @Test
    void toLocalDateTime_fromNull_returnsNull() {
        assertNull(DateCoercion.toLocalDateTime(SqlNull.INSTANCE));
    }

    // ── Integration: YEAR on ISO datetime string ─────────────────────────────

    @Test
    void integration_year_onIsoDatetimeString_returnsCorrectYear() {
        String json = """
                [
                  {"registered_at": "2021-08-19T00:00:00"},
                  {"registered_at": "2022-03-15T12:00:00"}
                ]""";
        String result = SQL4Json.query("SELECT YEAR(registered_at) AS yr FROM $r", json);
        // Both rows should produce a non-null year value
        assertFalse(result.contains("null"), "Expected no nulls in result, got: " + result);
        assertTrue(result.contains("2021"), "Expected 2021 in result, got: " + result);
        assertTrue(result.contains("2022"), "Expected 2022 in result, got: " + result);
    }

    @Test
    void integration_year_onIsoDateString_returnsCorrectYear() {
        String json = """
                [
                  {"registered_at": "2021-08-19"},
                  {"registered_at": "2022-03-15"}
                ]""";
        String result = SQL4Json.query("SELECT YEAR(registered_at) AS yr FROM $r", json);
        assertFalse(result.contains("null"), "Expected no nulls in result, got: " + result);
        assertTrue(result.contains("2021"), "Expected 2021 in result, got: " + result);
        assertTrue(result.contains("2022"), "Expected 2022 in result, got: " + result);
    }

    // ── Integration: DATE_DIFF on ISO string dates ────────────────────────────

    @Test
    void integration_dateDiff_onIsoDateStrings_returnsCorrectDays() {
        String json = """
                [
                  {"start": "2021-01-01", "end": "2021-01-11"}
                ]""";
        String result = SQL4Json.query(
                "SELECT DATE_DIFF(end, start, 'DAY') AS diff FROM $r", json);
        assertTrue(result.contains("10"), "Expected diff of 10 days, got: " + result);
    }

    @Test
    void integration_dateDiff_onIsoDatetimeStrings_returnsCorrectHours() {
        String json = """
                [
                  {"start": "2021-01-01T00:00:00", "end": "2021-01-01T06:00:00"}
                ]""";
        String result = SQL4Json.query(
                "SELECT DATE_DIFF(end, start, 'HOUR') AS diff FROM $r", json);
        assertTrue(result.contains("6"), "Expected diff of 6 hours, got: " + result);
    }

    // ── Integration: GROUP BY YEAR on ISO strings ─────────────────────────────

    @Test
    void integration_groupByYear_onIsoStrings_producesMultipleGroups() {
        String json = """
                [
                  {"registered_at": "2021-08-19T00:00:00"},
                  {"registered_at": "2021-09-01T00:00:00"},
                  {"registered_at": "2022-03-15T00:00:00"}
                ]""";
        String result = SQL4Json.query(
                "SELECT YEAR(registered_at) AS yr, COUNT(*) AS cnt FROM $r GROUP BY YEAR(registered_at)",
                json);
        // Should produce two groups: 2021 (count 2) and 2022 (count 1)
        // NOT collapsed into one null group
        assertTrue(result.contains("2021"), "Expected group for 2021, got: " + result);
        assertTrue(result.contains("2022"), "Expected group for 2022, got: " + result);
        assertFalse(result.contains("\"yr\":null"), "Groups should not be null, got: " + result);
    }

    // ── Integration: DATE_ADD on ISO string ──────────────────────────────────

    @Test
    void integration_dateAdd_onIsoDateString_returnsSqlDateTime() {
        String json = """
                [
                  {"dt": "2021-01-01T00:00:00"}
                ]""";
        String result = SQL4Json.query(
                "SELECT DATE_ADD(dt, 1, 'DAY') AS next FROM $r", json);
        assertTrue(result.contains("2021-01-02"), "Expected 2021-01-02 in result, got: " + result);
    }

    // ── Integration: MONTH / DAY on ISO strings ───────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = {"MONTH", "DAY", "HOUR", "MINUTE", "SECOND"})
    void integration_datePartFunction_onIsoDatetimeString_notNull(String fn) {
        String json = """
                [{"ts": "2021-08-19T10:30:45"}]""";
        String sql = "SELECT " + fn + "(ts) AS v FROM $r";
        String result = SQL4Json.query(sql, json);
        assertFalse(result.contains("null"), fn + " should not return null for ISO string, got: " + result);
    }
}
