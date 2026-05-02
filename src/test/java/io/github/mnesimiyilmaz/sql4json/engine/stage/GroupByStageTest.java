// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine.stage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.ColumnRef;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class GroupByStageTest {

    private static final int NO_LIMIT = Integer.MAX_VALUE;

    private final FunctionRegistry fn = FunctionRegistry.createDefault();

    @Test
    void groups_by_column_and_aggregates() {
        var it1 = Row.eager(
                Map.of(FieldKey.of("dept"), new SqlString("IT"), FieldKey.of("name"), new SqlString("Alice")));
        var it2 =
                Row.eager(Map.of(FieldKey.of("dept"), new SqlString("IT"), FieldKey.of("name"), new SqlString("Bob")));
        var hr1 = Row.eager(
                Map.of(FieldKey.of("dept"), new SqlString("HR"), FieldKey.of("name"), new SqlString("Carol")));

        var columns = List.of(SelectColumnDef.column("dept"), SelectColumnDef.aggregate("COUNT", "name", "cnt"));
        var stage = new GroupByStage(List.<Expression>of(new ColumnRef("dept")), columns, fn, NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(Stream.<RowAccessor>of(it1, it2, hr1)).toList();

        assertEquals(2, result.size());
        // GROUP BY emits FlatRow.aggregated, which carries the aggregated flag.
        result.forEach(r -> assertTrue(r.isAggregated()));
    }

    @Test
    void group_by_stage_is_not_lazy() {
        assertFalse(new GroupByStage(
                        List.<Expression>of(new ColumnRef("dept")),
                        List.of(SelectColumnDef.column("dept")),
                        fn,
                        NO_LIMIT)
                .isLazy());
    }
}
