// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.engine.WindowSpec;
import io.github.mnesimiyilmaz.sql4json.parser.OrderByColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WindowStageTest {

    private static final int NO_LIMIT = Integer.MAX_VALUE;

    private final FunctionRegistry registry = FunctionRegistry.getDefault();

    private Row row(String name, String dept, int salary) {
        return Row.eager(Map.of(
                FieldKey.of("name"), new SqlString(name),
                FieldKey.of("dept"), new SqlString(dept),
                FieldKey.of("salary"), SqlNumber.of(salary)));
    }

    private SqlValue windowResult(RowAccessor row, Expression.WindowFnCall wfc) {
        return row.getWindowResult(wfc);
    }

    private int intResult(RowAccessor row, Expression.WindowFnCall wfc) {
        return (int) ((SqlNumber) windowResult(row, wfc)).longValue();
    }

    @Test
    void row_number_assigns_sequential_numbers() {
        var rows = List.of(row("Bob", "Eng", 80000), row("Alice", "Eng", 90000), row("Charlie", "Eng", 70000));
        var winExpr = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        assertEquals(3, result.size());
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int rn = intResult(r, winExpr);
            switch (name) {
                case "Alice" -> assertEquals(1, rn);
                case "Bob" -> assertEquals(2, rn);
                case "Charlie" -> assertEquals(3, rn);
            }
        }
    }

    @Test
    void rank_handles_ties() {
        var rows = List.of(row("A", "X", 100), row("B", "X", 100), row("C", "X", 80));
        var winExpr = new Expression.WindowFnCall(
                "RANK", List.of(), new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int rnk = intResult(r, winExpr);
            switch (name) {
                case "A", "B" -> assertEquals(1, rnk);
                case "C" -> assertEquals(3, rnk);
            }
        }
    }

    @Test
    void dense_rank_no_gaps() {
        var rows = List.of(row("A", "X", 100), row("B", "X", 100), row("C", "X", 80));
        var winExpr = new Expression.WindowFnCall(
                "DENSE_RANK", List.of(), new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int drnk = intResult(r, winExpr);
            switch (name) {
                case "A", "B" -> assertEquals(1, drnk);
                case "C" -> assertEquals(2, drnk);
            }
        }
    }

    @Test
    void ntile_divides_into_buckets() {
        var rows =
                List.of(row("A", "X", 100), row("B", "X", 90), row("C", "X", 80), row("D", "X", 70), row("E", "X", 60));
        var winExpr = new Expression.WindowFnCall(
                "NTILE",
                List.of(new Expression.LiteralVal(SqlNumber.of(2))),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int half = intResult(r, winExpr);
            switch (name) {
                case "A", "B", "C" -> assertEquals(1, half);
                case "D", "E" -> assertEquals(2, half);
            }
        }
    }

    @Test
    void lag_returns_previous_row_value() {
        var rows = List.of(row("A", "X", 100), row("B", "X", 200), row("C", "X", 300));
        var winExpr = new Expression.WindowFnCall(
                "LAG",
                List.of(new Expression.ColumnRef("salary")),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "ASC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            SqlValue prev = windowResult(r, winExpr);
            switch (name) {
                case "A" -> assertEquals(SqlNull.INSTANCE, prev);
                case "B" -> assertEquals(SqlNumber.of(100), prev);
                case "C" -> assertEquals(SqlNumber.of(200), prev);
            }
        }
    }

    @Test
    void lead_returns_next_row_value() {
        var rows = List.of(row("A", "X", 100), row("B", "X", 200), row("C", "X", 300));
        var winExpr = new Expression.WindowFnCall(
                "LEAD",
                List.of(new Expression.ColumnRef("salary")),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "ASC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            SqlValue next = windowResult(r, winExpr);
            switch (name) {
                case "A" -> assertEquals(SqlNumber.of(200), next);
                case "B" -> assertEquals(SqlNumber.of(300), next);
                case "C" -> assertEquals(SqlNull.INSTANCE, next);
            }
        }
    }

    @Test
    void lag_with_custom_offset() {
        var rows = List.of(row("A", "X", 100), row("B", "X", 200), row("C", "X", 300), row("D", "X", 400));
        var winExpr = new Expression.WindowFnCall(
                "LAG",
                List.of(new Expression.ColumnRef("salary"), new Expression.LiteralVal(SqlNumber.of(2))),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "ASC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            SqlValue prev2 = windowResult(r, winExpr);
            switch (name) {
                case "A", "B" -> assertEquals(SqlNull.INSTANCE, prev2);
                case "C" -> assertEquals(SqlNumber.of(100), prev2);
                case "D" -> assertEquals(SqlNumber.of(200), prev2);
            }
        }
    }

    @Test
    void sum_over_partition() {
        var rows = List.of(row("A", "Eng", 100), row("B", "Eng", 200), row("C", "Mkt", 150));
        var winExpr = new Expression.WindowFnCall(
                "SUM",
                List.of(new Expression.ColumnRef("salary")),
                new WindowSpec(List.of(new Expression.ColumnRef("dept")), List.of()));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            double total = ((SqlNumber) windowResult(r, winExpr)).doubleValue();
            switch (name) {
                case "A", "B" -> assertEquals(300.0, total);
                case "C" -> assertEquals(150.0, total);
            }
        }
    }

    @Test
    void count_star_over_partition() {
        var rows = List.of(row("A", "Eng", 100), row("B", "Eng", 200), row("C", "Mkt", 150));
        var winExpr = new Expression.WindowFnCall(
                "COUNT", List.of(), new WindowSpec(List.of(new Expression.ColumnRef("dept")), List.of()));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        for (RowAccessor r : result) {
            String dept = ((SqlString) r.get(FieldKey.of("dept"))).value();
            int cnt = intResult(r, winExpr);
            switch (dept) {
                case "Eng" -> assertEquals(2, cnt);
                case "Mkt" -> assertEquals(1, cnt);
            }
        }
    }

    @Test
    void partition_by_preserves_original_row_order() {
        var rows = List.of(row("C", "Mkt", 150), row("A", "Eng", 100), row("B", "Eng", 200));
        var winExpr = new Expression.WindowFnCall(
                "ROW_NUMBER",
                List.of(),
                new WindowSpec(
                        List.of(new Expression.ColumnRef("dept")), List.of(OrderByColumnDef.of("salary", "ASC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        assertEquals("C", ((SqlString) result.get(0).get(FieldKey.of("name"))).value());
        assertEquals("A", ((SqlString) result.get(1).get(FieldKey.of("name"))).value());
        assertEquals("B", ((SqlString) result.get(2).get(FieldKey.of("name"))).value());
    }

    @Test
    void empty_input_returns_empty() {
        var winExpr = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "ASC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(java.util.stream.Stream.<RowAccessor>empty()).toList();
        assertTrue(result.isEmpty());
    }

    @Test
    void flatRow_input_uses_first_row_schema_directly() {
        // FlatRow stream (e.g. post-GROUP BY / post-JOIN) — schema is uniform; the
        // stage should reuse it without scanning every row.
        var schema = io.github.mnesimiyilmaz.sql4json.engine.RowSchema.of(
                List.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")));
        var fr1 = io.github.mnesimiyilmaz.sql4json.engine.FlatRow.of(
                schema, new Object[] {new SqlString("Alice"), new SqlString("Eng"), SqlNumber.of(100L)});
        var fr2 = io.github.mnesimiyilmaz.sql4json.engine.FlatRow.of(
                schema, new Object[] {new SqlString("Bob"), new SqlString("Eng"), SqlNumber.of(80L)});
        var winExpr = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var stage = new WindowStage(
                List.of(winExpr),
                List.of(),
                java.util.Set.of(FieldKey.of("name"), FieldKey.of("dept"), FieldKey.of("salary")),
                registry,
                NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(java.util.stream.Stream.<RowAccessor>of(fr1, fr2)).toList();
        assertEquals(2, result.size());
        // Verify the input schema's columns were preserved
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int rn = (int) ((SqlNumber) windowResult(r, winExpr)).longValue();
            switch (name) {
                case "Alice" -> assertEquals(1, rn);
                case "Bob" -> assertEquals(2, rn);
            }
        }
    }

    @Test
    void asterisk_select_falls_back_to_full_scan() {
        // SELECT * — referencedColumns can't statically enumerate input fields, so the
        // stage falls back to scanning every row's keys (the slow path). This exercises
        // the asterisk branch of collectBaseSchema.
        var asterisk = io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef.asterisk();
        var rows = List.of(row("Alice", "Eng", 100), row("Bob", "Eng", 80));
        var winExpr = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var stage = new WindowStage(List.of(winExpr), List.of(asterisk), java.util.Set.of(), registry, NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(rows.stream().map(r -> (RowAccessor) r)).toList();
        assertEquals(2, result.size());
        // All input fields should still be reachable via the scanned schema.
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int rn = (int) ((SqlNumber) windowResult(r, winExpr)).longValue();
            switch (name) {
                case "Alice" -> assertEquals(1, rn);
                case "Bob" -> assertEquals(2, rn);
            }
        }
    }

    @Test
    void lazyRow_input_uses_referenced_columns_when_no_asterisk() {
        // Lazy Row backed by a JsonValue tree, no SELECT * — the stage should use
        // the parser-collected referencedColumns to size its base schema rather
        // than triggering keys()-induced full flatten on every row.
        var jsonA = new io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue(new java.util.LinkedHashMap<>(Map.of(
                "name", new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("Alice"),
                "dept", new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("Eng"),
                "salary", new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(100L),
                "extra", new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("ignored"))));
        var jsonB = new io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue(new java.util.LinkedHashMap<>(Map.of(
                "name", new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("Bob"),
                "dept", new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("Eng"),
                "salary", new io.github.mnesimiyilmaz.sql4json.json.JsonLongValue(80L),
                "extra", new io.github.mnesimiyilmaz.sql4json.json.JsonStringValue("ignored"))));
        var interner = new FieldKey.Interner();
        var rA = Row.lazy(jsonA, interner);
        var rB = Row.lazy(jsonB, interner);
        var winExpr = new Expression.WindowFnCall(
                "ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        // referencedColumns covers what the query needs ("name", "salary" — partition/order/select)
        java.util.Set<FieldKey> referenced = java.util.Set.of(FieldKey.of("name"), FieldKey.of("salary"));
        var stage = new WindowStage(List.of(winExpr), List.of(), referenced, registry, NO_LIMIT);
        List<RowAccessor> result =
                stage.apply(java.util.stream.Stream.<RowAccessor>of(rA, rB)).toList();
        assertEquals(2, result.size());
        // Output FlatRow schema = referencedColumns + window slot — "extra" is dropped.
        for (RowAccessor r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int rn = (int) ((SqlNumber) windowResult(r, winExpr)).longValue();
            switch (name) {
                case "Alice" -> assertEquals(1, rn);
                case "Bob" -> assertEquals(2, rn);
            }
            // "extra" was not in referencedColumns — should resolve to SqlNull.
            assertEquals(SqlNull.INSTANCE, r.get(FieldKey.of("extra")));
        }
    }
}
