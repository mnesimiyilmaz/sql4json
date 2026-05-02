// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.sorting;

import io.github.mnesimiyilmaz.sql4json.types.*;

/**
 * Comparator for {@link SqlValue} instances with deterministic cross-type ordering. Nulls sort first; within the same
 * type, natural ordering applies.
 */
public final class SqlValueComparator {

    private SqlValueComparator() {}

    // Deterministic type ordering for cross-type comparisons.
    // Ensures Comparator contract: compare(a,b)==0 implies a.equals(b).
    // SQL engines use similar type ranking for ORDER BY stability.
    private static int typeOrdinal(SqlValue v) {
        return switch (v) {
            case SqlNull ignored -> 0;
            case SqlBoolean ignored -> 1;
            case SqlNumber ignored -> 2;
            case SqlString ignored -> 3;
            case SqlDate ignored -> 4;
            case SqlDateTime ignored -> 5;
        };
    }

    /**
     * Compares two SQL values with deterministic cross-type ordering.
     *
     * @param a the first value
     * @param b the second value
     * @return negative, zero, or positive as {@code a} is less than, equal to, or greater than {@code b}
     */
    public static int compare(SqlValue a, SqlValue b) {
        return switch (a) {
            case SqlNull ignored -> b.isNull() ? 0 : -1;
            case SqlNumber lhsN ->
                switch (b) {
                    case SqlLong(long rhsL) when lhsN instanceof SqlLong(long lhsL) -> Long.compare(lhsL, rhsL);
                    case SqlDouble(double rhsD) when lhsN instanceof SqlLong(long lhsL) -> Double.compare(lhsL, rhsD);
                    case SqlLong(long rhsL) when lhsN instanceof SqlDouble(double lhsD) -> Double.compare(lhsD, rhsL);
                    case SqlDouble(double rhsD)
                    when lhsN instanceof SqlDouble(double lhsD) -> Double.compare(lhsD, rhsD);
                    case SqlNumber rhsN -> Double.compare(lhsN.doubleValue(), rhsN.doubleValue());
                    case SqlNull ignored -> 1;
                    default -> Integer.compare(typeOrdinal(a), typeOrdinal(b));
                };
            case SqlString(var value) ->
                switch (b) {
                    case SqlString(var other) -> value.compareTo(other);
                    case SqlNull ignored -> 1;
                    default -> Integer.compare(typeOrdinal(a), typeOrdinal(b));
                };
            case SqlDate(var value) ->
                switch (b) {
                    case SqlDate(var other) -> value.compareTo(other);
                    case SqlNull ignored -> 1;
                    default -> Integer.compare(typeOrdinal(a), typeOrdinal(b));
                };
            case SqlDateTime(var value) ->
                switch (b) {
                    case SqlDateTime(var other) -> value.compareTo(other);
                    case SqlNull ignored -> 1;
                    default -> Integer.compare(typeOrdinal(a), typeOrdinal(b));
                };
            case SqlBoolean(var value) ->
                switch (b) {
                    case SqlBoolean(var other) -> Boolean.compare(value, other);
                    case SqlNull ignored -> 1;
                    default -> Integer.compare(typeOrdinal(a), typeOrdinal(b));
                };
        };
    }
}
