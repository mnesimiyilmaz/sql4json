// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Read-only row interface implemented by both {@link Row} (lazy) and {@link FlatRow} (materialized). Sealed so
 * consumers — evaluators, condition handlers, the unflattener — can switch exhaustively.
 *
 * @since 1.2.0
 */
public sealed interface RowAccessor permits Row, FlatRow {

    /**
     * Returns the value at the given field key, or {@link io.github.mnesimiyilmaz.sql4json.types.SqlNull#INSTANCE} if
     * absent.
     *
     * @param key the field key to look up
     * @return the value at {@code key}, or {@code SqlNull.INSTANCE}
     */
    SqlValue get(FieldKey key);

    /**
     * Returns whether this row was produced by GROUP BY aggregation. Aggregated rows are pre-evaluated against the
     * SELECT expression list and read flat by the unflattener; non-aggregated rows expose raw input fields.
     *
     * @return {@code true} for GROUP BY aggregated rows
     */
    boolean isAggregated();

    /**
     * Returns the source group of rows that produced this aggregated row, or empty for non-aggregated rows.
     *
     * @return the source group, or empty
     */
    Optional<List<RowAccessor>> sourceGroup();

    /**
     * Returns the precomputed window function value for the given call, or {@code null} if no value has been stored.
     *
     * @param call the window function call
     * @return the precomputed value or {@code null}
     */
    SqlValue getWindowResult(Expression.WindowFnCall call);

    /**
     * Returns whether any window result has been stored on this row.
     *
     * @return {@code true} when at least one window result is present
     */
    boolean hasWindowResults();

    /**
     * Returns the schema for this row. {@link FlatRow} returns the real schema; {@link Row} (lazy) returns {@code null}
     * — the legacy lazy row has no fixed schema. Callers must handle both cases.
     *
     * @return the schema, or {@code null} for legacy lazy rows
     */
    RowSchema schema();

    /**
     * Returns the original {@link JsonValue} backing this row, when available. Lazy {@link Row} instances retain it for
     * cherry-pick unflatten and array- predicate navigation; eager / pre-flattened {@link Row} and {@link FlatRow}
     * return empty (the parsed tree has been GC'd) unless explicitly retained by {@link FlatRow#materialize(Row,
     * RowSchema)}.
     *
     * <p>Used by the internal {@code ArrayPathNavigator} to navigate to array fields without rebuilding a
     * {@link JsonValue} tree.
     *
     * @return the original JSON value, or empty
     */
    Optional<JsonValue> originalValue();

    /**
     * Returns all values whose field key belongs to the given family — i.e. the array-index-stripped path. Used by the
     * internal {@code ArrayPathNavigator} for post-JOIN merged rows that have lost their original {@link JsonValue}.
     *
     * @param family the family path
     * @return the values; empty list when no family matches
     */
    List<SqlValue> valuesByFamily(String family);

    /**
     * Returns the set of {@link FieldKey}s exposed by this row. Lazy rows fully flatten on first call; {@link FlatRow}
     * returns its schema columns in source order.
     *
     * @return the field keys
     * @since 1.2.0
     */
    Set<FieldKey> keys();

    /**
     * Returns a stream of all field entries on this row. Lazy rows fully flatten on first call; {@link FlatRow} streams
     * its schema columns in order.
     *
     * @return a stream of field key-value entries
     * @since 1.2.0
     */
    Stream<Map.Entry<FieldKey, SqlValue>> entries();
}
