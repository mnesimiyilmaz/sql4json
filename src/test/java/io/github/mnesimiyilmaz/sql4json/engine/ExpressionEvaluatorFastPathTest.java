// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * Verifies the typed numeric fast paths added to {@link SqlValueComparator} — Long/Long, Long/Double, Double/Long,
 * Double/Double avoid the {@code doubleValue()} boxing fallback. Decimal involvement still routes via the generic
 * {@code Double.compare(doubleValue, doubleValue)} branch.
 *
 * @since 1.2.0
 */
class ExpressionEvaluatorFastPathTest {

    @Test
    void longLong_orderEqualEquivalent() {
        assertEquals(0, SqlValueComparator.compare(SqlNumber.of(5L), SqlNumber.of(5L)));
        assertTrue(SqlValueComparator.compare(SqlNumber.of(1L), SqlNumber.of(2L)) < 0);
        assertTrue(SqlValueComparator.compare(SqlNumber.of(2L), SqlNumber.of(1L)) > 0);
    }

    @Test
    void longDouble_compareViaDouble() {
        assertEquals(0, SqlValueComparator.compare(SqlNumber.of(5L), SqlNumber.of(5.0)));
        assertTrue(SqlValueComparator.compare(SqlNumber.of(5L), SqlNumber.of(5.1)) < 0);
        assertTrue(SqlValueComparator.compare(SqlNumber.of(5L), SqlNumber.of(4.9)) > 0);
    }

    @Test
    void doubleLong_compareViaDouble() {
        assertEquals(0, SqlValueComparator.compare(SqlNumber.of(5.0), SqlNumber.of(5L)));
        assertTrue(SqlValueComparator.compare(SqlNumber.of(4.9), SqlNumber.of(5L)) < 0);
        assertTrue(SqlValueComparator.compare(SqlNumber.of(5.1), SqlNumber.of(5L)) > 0);
    }

    @Test
    void doubleDouble_NaN_handling() {
        // Comparator contract: NaN sorts last (Double.compare semantics).
        assertTrue(SqlValueComparator.compare(SqlNumber.of(Double.NaN), SqlNumber.of(0.0)) > 0);
        assertTrue(SqlValueComparator.compare(SqlNumber.of(0.0), SqlNumber.of(Double.NaN)) < 0);
        assertEquals(0, SqlValueComparator.compare(SqlNumber.of(Double.NaN), SqlNumber.of(Double.NaN)));
    }

    @Test
    void doubleDouble_orderEqualEquivalent() {
        assertEquals(0, SqlValueComparator.compare(SqlNumber.of(3.14), SqlNumber.of(3.14)));
        assertTrue(SqlValueComparator.compare(SqlNumber.of(1.0), SqlNumber.of(2.0)) < 0);
    }

    @Test
    void decimalAny_fallsBackToDoubleCompare() {
        SqlValue d = SqlNumber.of(new BigDecimal("3.14"));
        assertEquals(0, SqlValueComparator.compare(d, SqlNumber.of(3.14)));
        assertEquals(0, SqlValueComparator.compare(SqlNumber.of(3.14), d));
    }

    @Test
    void decimalDecimal_compareViaDouble() {
        SqlValue d1 = SqlNumber.of(new BigDecimal("1.0"));
        SqlValue d2 = SqlNumber.of(new BigDecimal("2.0"));
        assertTrue(SqlValueComparator.compare(d1, d2) < 0);
    }

    @Test
    void decimalLong_compareViaDouble() {
        SqlValue d = SqlNumber.of(new BigDecimal("3.14"));
        assertTrue(SqlValueComparator.compare(d, SqlNumber.of(3L)) > 0);
        assertTrue(SqlValueComparator.compare(SqlNumber.of(3L), d) < 0);
    }

    @Test
    void anyNull_nullSortsFirst() {
        assertTrue(SqlValueComparator.compare(SqlNull.INSTANCE, SqlNumber.of(1L)) < 0);
        assertTrue(SqlValueComparator.compare(SqlNumber.of(1L), SqlNull.INSTANCE) > 0);
    }
}
