// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Immutable column-ordinal map. Each {@link FlatRow} carries a reference to a shared {@code RowSchema}; values are
 * stored in an {@code Object[]} keyed by column ordinal.
 *
 * <p>Provides three indexes built at construction:
 *
 * <ul>
 *   <li>{@code FieldKey} → ordinal for field-key lookups
 *   <li>family-name → ordinal[] for nested-array reconstruction in the unflattener
 *   <li>{@link Expression.WindowFnCall} → ordinal for window-result lookups (lazy; built only when
 *       {@link #withWindowSlots(List, Map)} is called)
 * </ul>
 *
 * @since 1.2.0
 */
public final class RowSchema {

    private final FieldKey[] columns;
    private final Map<FieldKey, Integer> index;
    private final Map<String, int[]> familyIndex;
    private final Map<Expression.WindowFnCall, Integer> windowSlots;

    private RowSchema(
            FieldKey[] columns,
            Map<FieldKey, Integer> index,
            Map<String, int[]> familyIndex,
            Map<Expression.WindowFnCall, Integer> windowSlots) {
        this.columns = columns;
        this.index = index;
        this.familyIndex = familyIndex;
        this.windowSlots = windowSlots;
    }

    /**
     * Builds a {@code RowSchema} from an ordered list of {@link FieldKey}.
     *
     * @param columnList ordered columns; must be non-null
     * @return the schema
     * @throws SQL4JsonExecutionException if a duplicate column key is present
     */
    public static RowSchema of(List<FieldKey> columnList) {
        FieldKey[] cols = columnList.toArray(new FieldKey[0]);
        Map<FieldKey, Integer> idx = HashMap.newHashMap(cols.length);
        for (int i = 0; i < cols.length; i++) {
            if (idx.putIfAbsent(cols[i], i) != null) {
                throw new SQL4JsonExecutionException("Duplicate column in RowSchema: " + cols[i].getKey());
            }
        }
        return new RowSchema(cols, Collections.unmodifiableMap(idx), buildFamilyIndex(cols), null);
    }

    private static Map<String, int[]> buildFamilyIndex(FieldKey[] columns) {
        Map<String, List<Integer>> tmp = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            tmp.computeIfAbsent(columns[i].getFamily(), k -> new ArrayList<>()).add(i);
        }
        Map<String, int[]> out = HashMap.newHashMap(tmp.size());
        tmp.forEach((fam, list) -> {
            int[] arr = new int[list.size()];
            for (int i = 0; i < list.size(); i++) arr[i] = list.get(i);
            out.put(fam, arr);
        });
        return Collections.unmodifiableMap(out);
    }

    /**
     * Number of columns in the schema.
     *
     * @return the column count
     */
    public int size() {
        return columns.length;
    }

    /**
     * Returns the {@link FieldKey} at the given ordinal.
     *
     * @param ordinal the ordinal in {@code [0, size())}
     * @return the field key
     */
    public FieldKey columnAt(int ordinal) {
        return columns[ordinal];
    }

    /**
     * Returns the ordinal for the given field key, or {@code -1} when absent.
     *
     * @param key the key to look up
     * @return the ordinal, or {@code -1}
     */
    public int indexOf(FieldKey key) {
        Integer i = index.get(key);
        return i == null ? -1 : i;
    }

    /**
     * Returns the ordinals belonging to a family (e.g. all members of a nested-array group), or an empty array if no
     * match.
     *
     * @param family the family path (array-index-stripped)
     * @return the column ordinals in source order
     */
    public int[] familyIndexes(String family) {
        return familyIndex.getOrDefault(family, new int[0]);
    }

    /**
     * Returns the slot ordinal for a window function call, when this schema was built with
     * {@link #withWindowSlots(List, Map)}.
     *
     * @param call the window function call
     * @return the slot ordinal, or empty
     */
    public OptionalInt windowSlot(Expression.WindowFnCall call) {
        if (windowSlots == null) return OptionalInt.empty();
        Integer i = windowSlots.get(call);
        return i == null ? OptionalInt.empty() : OptionalInt.of(i);
    }

    /**
     * Returns whether this schema includes window-result slots.
     *
     * @return {@code true} when {@link #withWindowSlots(List, Map)} has populated slots
     */
    public boolean hasWindowSlots() {
        return windowSlots != null && !windowSlots.isEmpty();
    }

    /**
     * Returns a new schema retaining only the given columns, in the iteration order of {@code keep} (use
     * {@link LinkedHashSet} when a deterministic column order is required).
     *
     * @param keep columns to retain (must be a subset of this schema)
     * @return the projected schema
     */
    public RowSchema project(Set<FieldKey> keep) {
        return RowSchema.of(new ArrayList<>(keep));
    }

    /**
     * Returns a new schema concatenating this schema with {@code other}. Used by JOIN. Throws if the two schemas share
     * a column key.
     *
     * @param other the schema to append
     * @return the concatenated schema
     * @throws SQL4JsonExecutionException on column-name collision
     */
    public RowSchema concat(RowSchema other) {
        var merged = new ArrayList<FieldKey>(columns.length + other.columns.length);
        Collections.addAll(merged, columns);
        for (FieldKey k : other.columns) {
            if (index.containsKey(k)) {
                throw new SQL4JsonExecutionException("JOIN schema collision on column: " + k.getKey());
            }
            merged.add(k);
        }
        return RowSchema.of(merged);
    }

    /**
     * Returns a new schema with window-result slots appended after the existing columns. The returned schema knows the
     * slot ordinal for each call. When a call appears in {@code aliasesByCall}, the alias is used as the column key for
     * the slot — so callers reading {@code row.get(aliasKey)} resolve to the slot value via the regular index. Calls
     * without an alias get a synthetic column key.
     *
     * @param calls window function calls in source order
     * @param aliasesByCall optional map of WindowFnCall → SELECT alias FieldKey
     * @return the schema with window slots
     */
    public RowSchema withWindowSlots(
            List<Expression.WindowFnCall> calls, Map<Expression.WindowFnCall, FieldKey> aliasesByCall) {
        if (calls.isEmpty()) {
            return new RowSchema(columns, index, familyIndex, Map.of());
        }
        var distinct = new LinkedHashSet<>(calls);
        FieldKey[] grown = Arrays.copyOf(columns, columns.length + distinct.size());
        Map<Expression.WindowFnCall, Integer> slots = HashMap.newHashMap(distinct.size());
        Map<FieldKey, Integer> grownIndex = new HashMap<>(index);
        int next = columns.length;
        for (Expression.WindowFnCall call : distinct) {
            FieldKey colKey = aliasesByCall != null && aliasesByCall.containsKey(call)
                    ? aliasesByCall.get(call)
                    : FieldKey.of("__win_" + System.identityHashCode(call));
            grown[next] = colKey;
            grownIndex.put(colKey, next);
            slots.put(call, next);
            next++;
        }
        return new RowSchema(
                grown,
                Collections.unmodifiableMap(grownIndex),
                buildFamilyIndex(grown),
                Collections.unmodifiableMap(slots));
    }

    /**
     * Convenience overload: equivalent to {@code withWindowSlots(calls, Map.of())} — every slot gets a synthetic column
     * key.
     *
     * @param calls window function calls in source order
     * @return the schema with window slots
     */
    public RowSchema withWindowSlots(List<Expression.WindowFnCall> calls) {
        return withWindowSlots(calls, Map.of());
    }

    /**
     * Returns the columns in source order.
     *
     * @return an unmodifiable list of columns
     */
    public List<FieldKey> columns() {
        return List.of(columns);
    }

    /** Builder for incremental schema construction (used during inline collection for {@code SELECT *}). */
    public static final class Builder {

        private final LinkedHashMap<FieldKey, Integer> seen = new LinkedHashMap<>();

        /** Creates an empty builder. */
        public Builder() {
            // Intentionally empty — the seen map is initialized via field initializer.
        }

        /**
         * Adds a column if not already present.
         *
         * @param k the column key
         * @return this builder
         */
        public Builder add(FieldKey k) {
            seen.putIfAbsent(k, seen.size());
            return this;
        }

        /**
         * Returns whether {@code k} is already in the builder.
         *
         * @param k the column key
         * @return {@code true} if present
         */
        public boolean contains(FieldKey k) {
            return seen.containsKey(k);
        }

        /**
         * Builds the schema.
         *
         * @return the schema
         */
        public RowSchema build() {
            return RowSchema.of(new ArrayList<>(seen.keySet()));
        }
    }
}
