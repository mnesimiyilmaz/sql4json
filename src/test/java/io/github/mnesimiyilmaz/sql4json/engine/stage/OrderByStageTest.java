// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine.stage;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.parser.OrderByColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class OrderByStageTest {

    private static final int NO_LIMIT = Integer.MAX_VALUE;

    private final FunctionRegistry fn = FunctionRegistry.createDefault();

    @Test
    void single_column_asc() {
        var row1 = Row.eager(Map.of(FieldKey.of("name"), new SqlString("Charlie")));
        var row2 = Row.eager(Map.of(FieldKey.of("name"), new SqlString("Alice")));
        var row3 = Row.eager(Map.of(FieldKey.of("name"), new SqlString("Bob")));

        List<RowAccessor> result = new OrderByStage(List.of(OrderByColumnDef.of("name", "ASC")), fn, NO_LIMIT)
                .apply(Stream.<RowAccessor>of(row1, row2, row3))
                .toList();

        assertEquals("Alice", ((SqlString) result.get(0).get(FieldKey.of("name"))).value());
        assertEquals("Bob", ((SqlString) result.get(1).get(FieldKey.of("name"))).value());
        assertEquals("Charlie", ((SqlString) result.get(2).get(FieldKey.of("name"))).value());
    }

    @Test
    void single_column_desc() {
        var row1 = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(10)));
        var row2 = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(30)));
        var row3 = Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(20)));

        List<RowAccessor> result = new OrderByStage(List.of(OrderByColumnDef.of("age", "DESC")), fn, NO_LIMIT)
                .apply(Stream.<RowAccessor>of(row1, row2, row3))
                .toList();

        assertEquals(30.0, ((SqlNumber) result.get(0).get(FieldKey.of("age"))).doubleValue());
        assertEquals(20.0, ((SqlNumber) result.get(1).get(FieldKey.of("age"))).doubleValue());
        assertEquals(10.0, ((SqlNumber) result.get(2).get(FieldKey.of("age"))).doubleValue());
    }

    @Test
    void multi_column_uses_second_as_tiebreaker() {
        var row1 =
                Row.eager(Map.of(FieldKey.of("dept"), new SqlString("IT"), FieldKey.of("name"), new SqlString("Zara")));
        var row2 = Row.eager(
                Map.of(FieldKey.of("dept"), new SqlString("HR"), FieldKey.of("name"), new SqlString("Alice")));
        var row3 =
                Row.eager(Map.of(FieldKey.of("dept"), new SqlString("IT"), FieldKey.of("name"), new SqlString("Adam")));

        List<RowAccessor> result = new OrderByStage(
                        List.of(OrderByColumnDef.of("dept", "ASC"), OrderByColumnDef.of("name", "ASC")), fn, NO_LIMIT)
                .apply(Stream.<RowAccessor>of(row1, row2, row3))
                .toList();

        // HR before IT; within IT: Adam before Zara
        assertEquals("HR", ((SqlString) result.get(0).get(FieldKey.of("dept"))).value());
        assertEquals("Adam", ((SqlString) result.get(1).get(FieldKey.of("name"))).value());
        assertEquals("Zara", ((SqlString) result.get(2).get(FieldKey.of("name"))).value());
    }

    @Test
    void order_by_stage_is_not_lazy() {
        assertFalse(new OrderByStage(List.of(OrderByColumnDef.of("name")), fn, NO_LIMIT).isLazy());
    }

    // ── TopN ORDER BY ───────────────────────────────────────────────────

    private static Row ageRow(int age) {
        return Row.eager(Map.of(FieldKey.of("age"), SqlNumber.of(age)));
    }

    private static int ageOf(RowAccessor r) {
        return (int) ((SqlNumber) r.get(FieldKey.of("age"))).doubleValue();
    }

    @Test
    void top_n_asc_returns_smallest_k() {
        var input = Stream.<RowAccessor>of(ageRow(30), ageRow(10), ageRow(50), ageRow(20), ageRow(40));

        List<RowAccessor> result = new TopNOrderByStage(List.of(OrderByColumnDef.of("age", "ASC")), 0, 2, fn, NO_LIMIT)
                .apply(input)
                .toList();

        assertEquals(2, result.size());
        assertEquals(10, ageOf(result.get(0)));
        assertEquals(20, ageOf(result.get(1)));
    }

    @Test
    void top_n_desc_returns_largest_k() {
        var input = Stream.<RowAccessor>of(ageRow(30), ageRow(10), ageRow(50), ageRow(20), ageRow(40));

        List<RowAccessor> result = new TopNOrderByStage(List.of(OrderByColumnDef.of("age", "DESC")), 0, 3, fn, NO_LIMIT)
                .apply(input)
                .toList();

        assertEquals(3, result.size());
        assertEquals(50, ageOf(result.get(0)));
        assertEquals(40, ageOf(result.get(1)));
        assertEquals(30, ageOf(result.get(2)));
    }

    @Test
    void top_n_with_offset_skips_first_k() {
        var input = Stream.<RowAccessor>of(ageRow(30), ageRow(10), ageRow(50), ageRow(20), ageRow(40));

        List<RowAccessor> result = new TopNOrderByStage(List.of(OrderByColumnDef.of("age", "ASC")), 2, 2, fn, NO_LIMIT)
                .apply(input)
                .toList();

        // ASC sorted: 10, 20, 30, 40, 50 → skip 2 → take 2 → 30, 40
        assertEquals(2, result.size());
        assertEquals(30, ageOf(result.get(0)));
        assertEquals(40, ageOf(result.get(1)));
    }

    @Test
    void top_n_limit_larger_than_input_returns_all_sorted() {
        var input = Stream.<RowAccessor>of(ageRow(30), ageRow(10), ageRow(20));

        List<RowAccessor> result = new TopNOrderByStage(
                        List.of(OrderByColumnDef.of("age", "ASC")), 0, 100, fn, NO_LIMIT)
                .apply(input)
                .toList();

        assertEquals(3, result.size());
        assertEquals(10, ageOf(result.get(0)));
        assertEquals(20, ageOf(result.get(1)));
        assertEquals(30, ageOf(result.get(2)));
    }

    @Test
    void top_n_zero_limit_returns_empty() {
        var input = Stream.<RowAccessor>of(ageRow(30), ageRow(10), ageRow(20));

        List<RowAccessor> result = new TopNOrderByStage(List.of(OrderByColumnDef.of("age", "ASC")), 0, 0, fn, NO_LIMIT)
                .apply(input)
                .toList();

        assertTrue(result.isEmpty());
    }

    @Test
    void top_n_offset_beyond_heap_returns_empty() {
        var input = Stream.<RowAccessor>of(ageRow(30), ageRow(10), ageRow(20));

        // Heap fills to 5, but only 3 rows actually arrive → offset 10 skips everything.
        List<RowAccessor> result = new TopNOrderByStage(List.of(OrderByColumnDef.of("age", "ASC")), 10, 5, fn, NO_LIMIT)
                .apply(input)
                .toList();

        assertTrue(result.isEmpty());
    }

    @Test
    void top_n_multi_column_tiebreaker() {
        var r1 =
                Row.eager(Map.of(FieldKey.of("dept"), new SqlString("IT"), FieldKey.of("name"), new SqlString("Zara")));
        var r2 = Row.eager(
                Map.of(FieldKey.of("dept"), new SqlString("HR"), FieldKey.of("name"), new SqlString("Alice")));
        var r3 =
                Row.eager(Map.of(FieldKey.of("dept"), new SqlString("IT"), FieldKey.of("name"), new SqlString("Adam")));

        List<RowAccessor> result = new TopNOrderByStage(
                        List.of(OrderByColumnDef.of("dept", "ASC"), OrderByColumnDef.of("name", "ASC")),
                        0,
                        2,
                        fn,
                        NO_LIMIT)
                .apply(Stream.<RowAccessor>of(r1, r2, r3))
                .toList();

        assertEquals(2, result.size());
        // Full order: (HR, Alice), (IT, Adam), (IT, Zara) → top 2
        assertEquals("HR", ((SqlString) result.get(0).get(FieldKey.of("dept"))).value());
        assertEquals("Alice", ((SqlString) result.get(0).get(FieldKey.of("name"))).value());
        assertEquals("IT", ((SqlString) result.get(1).get(FieldKey.of("dept"))).value());
        assertEquals("Adam", ((SqlString) result.get(1).get(FieldKey.of("name"))).value());
    }

    @Test
    void top_n_enforces_max_rows_on_input() {
        // 5 input rows, maxRows=3 — must throw before the 4th row is processed.
        var input = Stream.<RowAccessor>of(ageRow(30), ageRow(10), ageRow(50), ageRow(20), ageRow(40));

        var stage = new TopNOrderByStage(List.of(OrderByColumnDef.of("age", "ASC")), 0, 2, fn, 3);

        var ex = assertThrows(
                SQL4JsonExecutionException.class, () -> stage.apply(input).toList());
        assertTrue(ex.getMessage().contains("ORDER BY"));
        assertTrue(ex.getMessage().contains("3"));
    }

    @Test
    void top_n_stage_is_not_lazy() {
        assertFalse(new TopNOrderByStage(List.of(OrderByColumnDef.of("age", "ASC")), 0, 10, fn, NO_LIMIT).isLazy());
    }

    @Test
    void top_n_handles_limit_plus_offset_overflow() {
        // limit+offset overflows int — should still produce correct results (capped at maxRows).
        var input = Stream.<RowAccessor>of(ageRow(30), ageRow(10), ageRow(20));

        List<RowAccessor> result = new TopNOrderByStage(
                        List.of(OrderByColumnDef.of("age", "ASC")), Integer.MAX_VALUE - 1, Integer.MAX_VALUE, fn, 100)
                .apply(input)
                .toList();

        // Capacity capped at 100, but offset is MAX_VALUE-1 which exceeds the 3 heap rows,
        // so the slice is empty. The point of this test is that we don't crash.
        assertTrue(result.isEmpty());
    }
}
