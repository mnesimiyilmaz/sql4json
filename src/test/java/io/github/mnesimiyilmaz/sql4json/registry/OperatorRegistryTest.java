// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.types.*;
import java.util.function.BiPredicate;
import org.junit.jupiter.api.Test;

class OperatorRegistryTest {

    private final OperatorRegistry registry = OperatorRegistry.createDefault();

    // ── = (equals) ──────────────────────────────────────────────────────────

    @Test
    void equals_twoEqualNumbers() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertTrue(pred.test(SqlNumber.of(42), SqlNumber.of(42)));
    }

    @Test
    void equals_twoDifferentNumbers() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(SqlNumber.of(1), SqlNumber.of(2)));
    }

    @Test
    void equals_twoEqualStrings() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertTrue(pred.test(new SqlString("hello"), new SqlString("hello")));
    }

    @Test
    void equals_twoDifferentStrings() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(new SqlString("a"), new SqlString("b")));
    }

    @Test
    void equals_nullEqualNull() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(SqlNull.INSTANCE, SqlNull.INSTANCE));
    }

    @Test
    void equals_nullNotEqualNumber() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(SqlNull.INSTANCE, SqlNumber.of(1)));
    }

    @Test
    void equals_crossType_notEqual() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(SqlNumber.of(1), new SqlString("1")));
    }

    // ── != (not equals) ──────────────────────────────────────────────────────

    @Test
    void notEquals_differentValues() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("!=");
        assertTrue(pred.test(SqlNumber.of(1), SqlNumber.of(2)));
    }

    @Test
    void notEquals_sameValues() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("!=");
        assertFalse(pred.test(new SqlString("x"), new SqlString("x")));
    }

    // ── > (greater than) ─────────────────────────────────────────────────────

    @Test
    void greaterThan_number_true() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate(">");
        assertTrue(pred.test(SqlNumber.of(10), SqlNumber.of(5)));
    }

    @Test
    void greaterThan_number_false() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate(">");
        assertFalse(pred.test(SqlNumber.of(5), SqlNumber.of(10)));
    }

    @Test
    void greaterThan_equal_false() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate(">");
        assertFalse(pred.test(SqlNumber.of(5), SqlNumber.of(5)));
    }

    // ── < (less than) ────────────────────────────────────────────────────────

    @Test
    void lessThan_number_true() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("<");
        assertTrue(pred.test(SqlNumber.of(3), SqlNumber.of(7)));
    }

    @Test
    void lessThan_strings_lexicographic() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("<");
        assertTrue(pred.test(new SqlString("apple"), new SqlString("banana")));
    }

    // ── >= (greater than or equal) ───────────────────────────────────────────

    @Test
    void greaterThanOrEqual_equal() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate(">=");
        assertTrue(pred.test(SqlNumber.of(5), SqlNumber.of(5)));
    }

    @Test
    void greaterThanOrEqual_greater() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate(">=");
        assertTrue(pred.test(SqlNumber.of(6), SqlNumber.of(5)));
    }

    @Test
    void greaterThanOrEqual_less() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate(">=");
        assertFalse(pred.test(SqlNumber.of(4), SqlNumber.of(5)));
    }

    // ── <= (less than or equal) ──────────────────────────────────────────────

    @Test
    void lessThanOrEqual_equal() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("<=");
        assertTrue(pred.test(SqlNumber.of(5), SqlNumber.of(5)));
    }

    @Test
    void lessThanOrEqual_less() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("<=");
        assertTrue(pred.test(SqlNumber.of(3), SqlNumber.of(5)));
    }

    // ── Unknown operator ─────────────────────────────────────────────────────

    @Test
    void unknownOperator_throwsException() {
        assertThrows(SQL4JsonExecutionException.class, () -> registry.getPredicate("BETWEEN"));
    }

    // ── Registration + lookup ────────────────────────────────────────────────

    @Test
    void register_customOperator_retrievable() {
        var customRegistry = new OperatorRegistry();
        customRegistry.register(new ComparisonOperatorDef("~~", OperatorType.BINARY, (a, b) -> true));
        assertNotNull(customRegistry.getPredicate("~~"));
    }

    // ── SqlBoolean equality ─────────────────────────────────────────────────

    @Test
    void equals_booleans_sameValue() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertTrue(pred.test(SqlBoolean.TRUE, SqlBoolean.TRUE));
        assertTrue(pred.test(SqlBoolean.FALSE, SqlBoolean.FALSE));
    }

    @Test
    void equals_booleans_differentValue() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(SqlBoolean.TRUE, SqlBoolean.FALSE));
    }

    @Test
    void equals_boolean_crossType() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(SqlBoolean.TRUE, SqlNumber.of(1)));
    }

    // ── SqlDate equality ────────────────────────────────────────────────────

    @Test
    void equals_dates_same() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertTrue(pred.test(
                new SqlDate(java.time.LocalDate.of(2024, 1, 1)), new SqlDate(java.time.LocalDate.of(2024, 1, 1))));
    }

    @Test
    void equals_dates_different() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(
                new SqlDate(java.time.LocalDate.of(2024, 1, 1)), new SqlDate(java.time.LocalDate.of(2024, 1, 2))));
    }

    @Test
    void equals_date_crossType() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(new SqlDate(java.time.LocalDate.of(2024, 1, 1)), new SqlString("2024-01-01")));
    }

    // ── SqlDateTime equality ────────────────────────────────────────────────

    @Test
    void equals_dateTimes_same() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertTrue(pred.test(
                new SqlDateTime(java.time.LocalDateTime.of(2024, 1, 1, 10, 0)),
                new SqlDateTime(java.time.LocalDateTime.of(2024, 1, 1, 10, 0))));
    }

    @Test
    void equals_dateTimes_different() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(
                new SqlDateTime(java.time.LocalDateTime.of(2024, 1, 1, 10, 0)),
                new SqlDateTime(java.time.LocalDateTime.of(2024, 1, 1, 11, 0))));
    }

    @Test
    void equals_dateTime_crossType() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(
                new SqlDateTime(java.time.LocalDateTime.of(2024, 1, 1, 10, 0)),
                new SqlDate(java.time.LocalDate.of(2024, 1, 1))));
    }

    // ── != with non-string/number types ─────────────────────────────────────

    @Test
    void notEquals_booleans() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("!=");
        assertTrue(pred.test(SqlBoolean.TRUE, SqlBoolean.FALSE));
        assertFalse(pred.test(SqlBoolean.TRUE, SqlBoolean.TRUE));
    }

    @Test
    void notEquals_dates() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("!=");
        assertTrue(pred.test(
                new SqlDate(java.time.LocalDate.of(2024, 1, 1)), new SqlDate(java.time.LocalDate.of(2024, 1, 2))));
    }

    @Test
    void notEquals_null() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("!=");
        // null != null is true (because = returns false for nulls)
        assertTrue(pred.test(SqlNull.INSTANCE, SqlNull.INSTANCE));
    }

    // ── String comparison with ordering operators ───────────────────────────

    @Test
    void greaterThanOrEqual_strings() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate(">=");
        assertTrue(pred.test(new SqlString("b"), new SqlString("a")));
        assertTrue(pred.test(new SqlString("a"), new SqlString("a")));
    }

    @Test
    void lessThanOrEqual_strings() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("<=");
        assertTrue(pred.test(new SqlString("a"), new SqlString("b")));
        assertTrue(pred.test(new SqlString("a"), new SqlString("a")));
    }

    // ── Number comparison with crosstype ─────────────────────────────────

    @Test
    void equals_number_crossType_string() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(SqlNumber.of(42), new SqlString("42")));
    }

    @Test
    void equals_string_crossType_number() {
        BiPredicate<SqlValue, SqlValue> pred = registry.getPredicate("=");
        assertFalse(pred.test(new SqlString("42"), SqlNumber.of(42)));
    }
}
