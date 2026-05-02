// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.FlatRow;
import io.github.mnesimiyilmaz.sql4json.engine.LazyPipelineStage;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.engine.RowSchema;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Stream;

/**
 * Lazy pipeline stage that projects rows to selected columns (SELECT clause).
 *
 * <p>Since 1.2.0 the stage emits {@link FlatRow} per non-aggregated, non-window row using a single shared
 * {@link RowSchema} derived from the SELECT column list. The original
 * {@link io.github.mnesimiyilmaz.sql4json.types.JsonValue} (when available on the input lazy {@link Row}) is retained
 * on the output so the unflattener can re-derive any column for computed expressions like {@code CONCAT(name, ' - ',
 * dept)}.
 *
 * <p>Pass-through cases:
 *
 * <ul>
 *   <li>SELECT * / window-bearing SELECT: {@code projectionSchema == null}.
 *   <li>Aggregated rows from GROUP BY: already carry the SELECT projection in their schema; passing through preserves
 *       the aggregated flag and source group needed for HAVING aggregate re-evaluation.
 *   <li>Window-bearing rows (rows that have window results stored, regardless of whether the SELECT def's
 *       {@code containsWindow()} reports them): projection would strip the window slots and break the unflattener's
 *       schema-indexed window lookup. Window functions buried in CASE WHEN conditions slip past
 *       {@code containsWindow()} (it walks the Expression tree but not the CriteriaNode hierarchy), so the per-row
 *       check is the safety net.
 * </ul>
 */
public final class SelectStage implements LazyPipelineStage {

    // null means SELECT * / window-bearing — pass through unchanged
    private final RowSchema projectionSchema;

    /**
     * Creates a SelectStage for the given column definitions.
     *
     * @param columns the SELECT column definitions
     */
    public SelectStage(List<SelectColumnDef> columns) {
        boolean isSelectAll = columns.size() == 1 && columns.getFirst().isAsterisk();
        // When any column contains a window function, skip projection. The outer
        // (possibly scalar / CASE) expression that wraps the window can reference any
        // source field, so dropping fields here would lose data needed by
        // JsonUnflattener / StreamingSerializer when they re-evaluate the column
        // expression. Window results live in per-row schema slots managed by
        // WindowStage and are unaffected by projection in either direction.
        boolean hasWindow = columns.stream().anyMatch(SelectColumnDef::containsWindow);
        if (isSelectAll || hasWindow) {
            this.projectionSchema = null;
        } else {
            // Only non-aggregate, non-asterisk columns with a column path need projection.
            // Computed expressions (e.g. CAST('literal' AS TYPE)) have no column path.
            LinkedHashSet<FieldKey> keys = new LinkedHashSet<>();
            for (SelectColumnDef c : columns) {
                if (!c.isAsterisk() && !c.containsAggregate() && c.columnName() != null) {
                    keys.add(FieldKey.of(c.columnName()));
                }
            }
            this.projectionSchema = keys.isEmpty() ? null : RowSchema.of(List.copyOf(keys));
        }
    }

    @Override
    public Stream<RowAccessor> apply(Stream<RowAccessor> input) {
        if (projectionSchema == null) {
            return input; // SELECT * / window-bearing — pass through
        }
        return input.map(this::project);
    }

    private RowAccessor project(RowAccessor row) {
        // Only lazy rows go through SelectStage projection. FlatRow inputs come
        // from upstream materialising stages (JOIN, GROUP BY, WindowStage,
        // engine pre-flatten) and already carry every column they need; passing
        // them through preserves the legacy behaviour where DISTINCT after a
        // JOIN compares the full merged row, not just the projected columns.
        if (!(row instanceof Row lazy)) return row;
        // Materialise the lazy row into a FlatRow with the projection schema,
        // retaining the original JsonValue so the unflattener can re-derive any
        // column needed by a computed expression like {@code CONCAT(name, ' - ', dept)}.
        return FlatRow.materialize(lazy, projectionSchema);
    }
}
