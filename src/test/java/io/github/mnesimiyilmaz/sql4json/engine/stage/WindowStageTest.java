package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.engine.WindowSpec;
import io.github.mnesimiyilmaz.sql4json.parser.OrderByColumnDef;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowStageTest {

    private static final int NO_LIMIT = Integer.MAX_VALUE;

    private final FunctionRegistry registry = FunctionRegistry.getDefault();

    private Row row(String name, String dept, int salary) {
        return Row.eager(Map.of(
                FieldKey.of("name"), new SqlString(name),
                FieldKey.of("dept"), new SqlString(dept),
                FieldKey.of("salary"), SqlNumber.of(salary)));
    }

    private SqlValue getWindowResult(Row row, String key) {
        return row.get(FieldKey.of(key));
    }

    private int intResult(Row row, String key) {
        return ((SqlNumber) getWindowResult(row, key)).value().intValue();
    }

    @Test
    void row_number_assigns_sequential_numbers() {
        var rows = List.of(row("Bob", "Eng", 80000), row("Alice", "Eng", 90000), row("Charlie", "Eng", 70000));
        var winExpr = new Expression.WindowFnCall("ROW_NUMBER", List.of(),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "row_num"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        assertEquals(3, result.size());
        for (Row r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int rn = intResult(r, "row_num");
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
        var winExpr = new Expression.WindowFnCall("RANK", List.of(),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "rnk"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        for (Row r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int rnk = intResult(r, "rnk");
            switch (name) {
                case "A", "B" -> assertEquals(1, rnk);
                case "C" -> assertEquals(3, rnk);
            }
        }
    }

    @Test
    void dense_rank_no_gaps() {
        var rows = List.of(row("A", "X", 100), row("B", "X", 100), row("C", "X", 80));
        var winExpr = new Expression.WindowFnCall("DENSE_RANK", List.of(),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "drnk"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        for (Row r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int drnk = intResult(r, "drnk");
            switch (name) {
                case "A", "B" -> assertEquals(1, drnk);
                case "C" -> assertEquals(2, drnk);
            }
        }
    }

    @Test
    void ntile_divides_into_buckets() {
        var rows = List.of(row("A", "X", 100), row("B", "X", 90), row("C", "X", 80),
                row("D", "X", 70), row("E", "X", 60));
        var winExpr = new Expression.WindowFnCall("NTILE",
                List.of(new Expression.LiteralVal(SqlNumber.of(2))),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "DESC"))));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "half"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        for (Row r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            int half = intResult(r, "half");
            switch (name) {
                case "A", "B", "C" -> assertEquals(1, half);
                case "D", "E" -> assertEquals(2, half);
            }
        }
    }

    @Test
    void lag_returns_previous_row_value() {
        var rows = List.of(row("A", "X", 100), row("B", "X", 200), row("C", "X", 300));
        var winExpr = new Expression.WindowFnCall("LAG",
                List.of(new Expression.ColumnRef("salary")),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "ASC"))));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "prev"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        for (Row r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            SqlValue prev = getWindowResult(r, "prev");
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
        var winExpr = new Expression.WindowFnCall("LEAD",
                List.of(new Expression.ColumnRef("salary")),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "ASC"))));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "next_sal"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        for (Row r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            SqlValue next = getWindowResult(r, "next_sal");
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
        var winExpr = new Expression.WindowFnCall("LAG",
                List.of(new Expression.ColumnRef("salary"), new Expression.LiteralVal(SqlNumber.of(2))),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "ASC"))));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "prev2"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        for (Row r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            SqlValue prev2 = getWindowResult(r, "prev2");
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
        var winExpr = new Expression.WindowFnCall("SUM",
                List.of(new Expression.ColumnRef("salary")),
                new WindowSpec(List.of(new Expression.ColumnRef("dept")), List.of()));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "dept_total"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        for (Row r : result) {
            String name = ((SqlString) r.get(FieldKey.of("name"))).value();
            double total = ((SqlNumber) getWindowResult(r, "dept_total")).value().doubleValue();
            switch (name) {
                case "A", "B" -> assertEquals(300.0, total);
                case "C" -> assertEquals(150.0, total);
            }
        }
    }

    @Test
    void count_star_over_partition() {
        var rows = List.of(row("A", "Eng", 100), row("B", "Eng", 200), row("C", "Mkt", 150));
        var winExpr = new Expression.WindowFnCall("COUNT", List.of(),
                new WindowSpec(List.of(new Expression.ColumnRef("dept")), List.of()));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "cnt"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        for (Row r : result) {
            String dept = ((SqlString) r.get(FieldKey.of("dept"))).value();
            int cnt = intResult(r, "cnt");
            switch (dept) {
                case "Eng" -> assertEquals(2, cnt);
                case "Mkt" -> assertEquals(1, cnt);
            }
        }
    }

    @Test
    void partition_by_preserves_original_row_order() {
        var rows = List.of(row("C", "Mkt", 150), row("A", "Eng", 100), row("B", "Eng", 200));
        var winExpr = new Expression.WindowFnCall("ROW_NUMBER", List.of(),
                new WindowSpec(List.of(new Expression.ColumnRef("dept")),
                        List.of(OrderByColumnDef.of("salary", "ASC"))));
        var cols = List.of(SelectColumnDef.column("name"), SelectColumnDef.of(winExpr, "rn"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(rows.stream()).toList();
        assertEquals("C", ((SqlString) result.get(0).get(FieldKey.of("name"))).value());
        assertEquals("A", ((SqlString) result.get(1).get(FieldKey.of("name"))).value());
        assertEquals("B", ((SqlString) result.get(2).get(FieldKey.of("name"))).value());
    }

    @Test
    void empty_input_returns_empty() {
        var winExpr = new Expression.WindowFnCall("ROW_NUMBER", List.of(),
                new WindowSpec(List.of(), List.of(OrderByColumnDef.of("salary", "ASC"))));
        var cols = List.of(SelectColumnDef.of(winExpr, "rn"));
        var stage = new WindowStage(cols, registry, NO_LIMIT);
        List<Row> result = stage.apply(java.util.stream.Stream.<Row>empty()).toList();
        assertTrue(result.isEmpty());
    }
}
