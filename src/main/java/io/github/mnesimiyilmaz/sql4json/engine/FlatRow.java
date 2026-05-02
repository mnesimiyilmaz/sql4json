// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Materialized row backed by an {@code Object[]} keyed by ordinal. Replaces the {@code HashMap<FieldKey, SqlValue>}
 * backing of legacy {@link Row} at every materialization boundary in the query pipeline.
 *
 * <p>Encoding: a {@code null} slot represents {@link SqlNull#INSTANCE}; the accessor materialises the singleton on
 * read. This saves one reference write per null slot during materialization.
 *
 * @since 1.2.0
 */
public final class FlatRow implements RowAccessor {

    private final RowSchema schema;
    private final Object[] values;
    private final boolean aggregated;
    private final List<RowAccessor> sourceGroup;
    private final JsonValue original;

    private FlatRow(
            RowSchema schema, Object[] values, boolean aggregated, List<RowAccessor> sourceGroup, JsonValue original) {
        this.schema = schema;
        this.values = values;
        this.aggregated = aggregated;
        this.sourceGroup = sourceGroup;
        this.original = original;
    }

    /**
     * Wraps the given {@code Object[]} in a {@code FlatRow} using {@code schema}. The array is captured by reference —
     * callers must not mutate after wrapping.
     *
     * @param schema the schema (must match {@code values.length})
     * @param values the value array (slot {@code i} corresponds to {@code schema.columnAt(i)})
     * @return a new {@code FlatRow}
     */
    public static FlatRow of(RowSchema schema, Object[] values) {
        return new FlatRow(schema, values, false, null, null);
    }

    /**
     * Wraps a value array as an aggregated row, retaining the source group for post-HAVING aggregate re-evaluation.
     *
     * @param schema the schema
     * @param values the value array
     * @param sourceGroup the source rows that aggregated to this row
     * @return a new aggregated {@code FlatRow}
     */
    public static FlatRow aggregated(RowSchema schema, Object[] values, List<? extends RowAccessor> sourceGroup) {
        return new FlatRow(schema, values, true, List.copyOf(sourceGroup), null);
    }

    /**
     * Wraps a value array as a pre-flattened engine row. The {@link JsonValue} tree has been GC'd by this point.
     *
     * @param schema the schema
     * @param values the value array
     * @return a new pre-flattened {@code FlatRow}
     */
    public static FlatRow preFlattened(RowSchema schema, Object[] values) {
        return new FlatRow(schema, values, false, null, null);
    }

    /**
     * Materialises a lazy {@link Row} into a {@code FlatRow} using the given schema. The original {@link JsonValue} is
     * retained for cherry-pick fallback in the unflattener.
     *
     * @param source the source lazy row
     * @param schema the target schema
     * @return a new {@code FlatRow}
     */
    public static FlatRow materialize(Row source, RowSchema schema) {
        Object[] vals = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            FieldKey k = schema.columnAt(i);
            SqlValue v = source.get(k);
            if (!(v instanceof SqlNull)) {
                vals[i] = v;
            }
        }
        JsonValue orig = source.originalValue().orElse(null);
        return new FlatRow(schema, vals, false, null, orig);
    }

    @Override
    public RowSchema schema() {
        return schema;
    }

    /**
     * Returns the value at the given ordinal, or {@link SqlNull#INSTANCE} when the slot is empty.
     *
     * @param ordinal the column ordinal
     * @return the value, never {@code null}
     */
    public SqlValue get(int ordinal) {
        Object v = values[ordinal];
        return v == null ? SqlNull.INSTANCE : (SqlValue) v;
    }

    @Override
    public SqlValue get(FieldKey key) {
        int idx = schema.indexOf(key);
        if (idx < 0) return SqlNull.INSTANCE;
        return get(idx);
    }

    @Override
    public List<SqlValue> valuesByFamily(String family) {
        int[] ordinals = schema.familyIndexes(family);
        if (ordinals.length == 0) return List.of();
        List<SqlValue> out = new ArrayList<>(ordinals.length);
        for (int o : ordinals) out.add(get(o));
        return out;
    }

    @Override
    public boolean isAggregated() {
        return aggregated;
    }

    @Override
    public Optional<List<RowAccessor>> sourceGroup() {
        return Optional.ofNullable(sourceGroup);
    }

    @Override
    public SqlValue getWindowResult(Expression.WindowFnCall call) {
        OptionalInt slot = schema.windowSlot(call);
        return slot.isPresent() ? get(slot.getAsInt()) : null;
    }

    @Override
    public boolean hasWindowResults() {
        return schema.hasWindowSlots();
    }

    @Override
    public Optional<JsonValue> originalValue() {
        return Optional.ofNullable(original);
    }

    @Override
    public Set<FieldKey> keys() {
        return new LinkedHashSet<>(schema.columns());
    }

    @Override
    public Stream<Map.Entry<FieldKey, SqlValue>> entries() {
        return IntStream.range(0, schema.size()).mapToObj(i -> Map.entry(schema.columnAt(i), get(i)));
    }
}
