package io.github.mnesimiyilmaz.sql4json.sorting;

import io.github.mnesimiyilmaz.sql4json.types.*;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlValueComparatorTest {

    // Null ordering: null < everything
    @Test
    void null_lessThan_number() {
        assertTrue(SqlValueComparator.compare(SqlNull.INSTANCE, SqlNumber.of(1)) < 0);
    }

    @Test
    void null_lessThan_string() {
        assertTrue(SqlValueComparator.compare(SqlNull.INSTANCE, new SqlString("a")) < 0);
    }

    @Test
    void null_equalsNull() {
        assertEquals(0, SqlValueComparator.compare(SqlNull.INSTANCE, SqlNull.INSTANCE));
    }

    @Test
    void number_greaterThan_null() {
        assertTrue(SqlValueComparator.compare(SqlNumber.of(1), SqlNull.INSTANCE) > 0);
    }

    // Same-type: numbers
    @Test
    void numbers_ascending() {
        assertTrue(SqlValueComparator.compare(SqlNumber.of(1), SqlNumber.of(2)) < 0);
        assertTrue(SqlValueComparator.compare(SqlNumber.of(2), SqlNumber.of(1)) > 0);
        assertEquals(0, SqlValueComparator.compare(SqlNumber.of(5), SqlNumber.of(5)));
    }

    @Test
    void numbers_decimalComparison() {
        assertTrue(SqlValueComparator.compare(SqlNumber.of(1.5), SqlNumber.of(2.0)) < 0);
    }

    // Same-type: strings
    @Test
    void strings_lexicographic() {
        assertTrue(SqlValueComparator.compare(new SqlString("apple"), new SqlString("banana")) < 0);
        assertTrue(SqlValueComparator.compare(new SqlString("z"), new SqlString("a")) > 0);
        assertEquals(0, SqlValueComparator.compare(new SqlString("same"), new SqlString("same")));
    }

    // Same-type: booleans
    @Test
    void booleans_falseBeforeTrue() {
        assertTrue(SqlValueComparator.compare(SqlBoolean.FALSE, SqlBoolean.TRUE) < 0);
        assertTrue(SqlValueComparator.compare(SqlBoolean.TRUE, SqlBoolean.FALSE) > 0);
        assertEquals(0, SqlValueComparator.compare(SqlBoolean.TRUE, SqlBoolean.TRUE));
    }

    // Same-type: dates
    @Test
    void dates_chronological() {
        SqlDate d1 = new SqlDate(LocalDate.of(2024, 1, 1));
        SqlDate d2 = new SqlDate(LocalDate.of(2024, 6, 1));
        assertTrue(SqlValueComparator.compare(d1, d2) < 0);
        assertTrue(SqlValueComparator.compare(d2, d1) > 0);
        assertEquals(0, SqlValueComparator.compare(d1, d1));
    }

    // Same-type: datetimes
    @Test
    void datetimes_chronological() {
        SqlDateTime dt1 = new SqlDateTime(LocalDateTime.of(2024, 1, 1, 0, 0));
        SqlDateTime dt2 = new SqlDateTime(LocalDateTime.of(2024, 1, 1, 12, 0));
        assertTrue(SqlValueComparator.compare(dt1, dt2) < 0);
    }

    // Cross-type ordering: uses type ordinal (deterministic, consistent)
    @Test
    void crossType_number_lessThan_string() {
        // Type ordinals: Null=0, Boolean=1, Number=2, String=3, Date=4, DateTime=5
        int result = SqlValueComparator.compare(SqlNumber.of(100), new SqlString("a"));
        assertTrue(result < 0, "Number should be < String in cross-type ordering");
    }

    @Test
    void crossType_string_greaterThan_boolean() {
        int result = SqlValueComparator.compare(new SqlString("true"), SqlBoolean.TRUE);
        assertTrue(result > 0, "String should be > Boolean in cross-type ordering");
    }

    // Cross-type: Date/DateTime/Boolean vs other types
    @Test
    void crossType_date_greaterThan_string() {
        SqlDate d = new SqlDate(LocalDate.of(2024, 1, 1));
        assertTrue(SqlValueComparator.compare(d, new SqlString("a")) > 0);
    }

    @Test
    void crossType_dateTime_greaterThan_date() {
        SqlDateTime dt = new SqlDateTime(LocalDateTime.of(2024, 1, 1, 0, 0));
        SqlDate d = new SqlDate(LocalDate.of(2024, 1, 1));
        assertTrue(SqlValueComparator.compare(dt, d) > 0);
    }

    @Test
    void crossType_boolean_lessThan_number() {
        assertTrue(SqlValueComparator.compare(SqlBoolean.TRUE, SqlNumber.of(1)) < 0);
    }

    @Test
    void crossType_boolean_greaterThan_null() {
        assertTrue(SqlValueComparator.compare(SqlBoolean.TRUE, SqlNull.INSTANCE) > 0);
    }

    @Test
    void crossType_date_greaterThan_null() {
        SqlDate d = new SqlDate(LocalDate.of(2024, 1, 1));
        assertTrue(SqlValueComparator.compare(d, SqlNull.INSTANCE) > 0);
    }

    @Test
    void crossType_dateTime_greaterThan_null() {
        SqlDateTime dt = new SqlDateTime(LocalDateTime.of(2024, 1, 1, 0, 0));
        assertTrue(SqlValueComparator.compare(dt, SqlNull.INSTANCE) > 0);
    }

    @Test
    void crossType_string_greaterThan_null() {
        assertTrue(SqlValueComparator.compare(new SqlString("a"), SqlNull.INSTANCE) > 0);
    }

    // Comparator contract: antisymmetry
    @Test
    void antisymmetry() {
        SqlNumber a = SqlNumber.of(5);
        SqlNumber b = SqlNumber.of(10);
        int ab = SqlValueComparator.compare(a, b);
        int ba = SqlValueComparator.compare(b, a);
        assertTrue(ab < 0 && ba > 0);
    }
}
