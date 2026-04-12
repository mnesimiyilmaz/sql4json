package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.LazyPipelineStage;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Lazy pipeline stage that projects rows to selected columns (SELECT clause).
 */
public final class SelectStage implements LazyPipelineStage {

    // null means SELECT * — pass through unchanged
    private final Set<FieldKey> selectedKeys;

    /**
     * Creates a SelectStage for the given column definitions.
     *
     * @param columns the SELECT column definitions
     */
    public SelectStage(List<SelectColumnDef> columns) {
        boolean isSelectAll = columns.size() == 1 && columns.getFirst().isAsterisk();
        if (isSelectAll) {
            this.selectedKeys = null;
        } else {
            // Only non-aggregate, non-asterisk columns with a column path need projection.
            // Computed expressions (e.g. CAST('literal' AS TYPE)) have no column path.
            this.selectedKeys = columns.stream()
                    .filter(c -> !c.isAsterisk() && !c.containsAggregate())
                    .filter(c -> c.containsWindow() || c.columnName() != null)
                    .map(c -> c.containsWindow()
                            ? FieldKey.of(c.aliasOrName())
                            : FieldKey.of(c.columnName()))
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public Stream<Row> apply(Stream<Row> input) {
        if (selectedKeys == null) {
            return input; // SELECT * — pass through
        }
        // Modified (aggregated) rows already have the right fields; only project lazy rows
        return input.map(row -> row.isModified() ? row : row.project(selectedKeys));
    }
}
