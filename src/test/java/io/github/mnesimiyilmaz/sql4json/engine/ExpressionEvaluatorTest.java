// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.mnesimiyilmaz.sql4json.engine.Expression.*;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExpressionEvaluatorTest {

    private static final FunctionRegistry FUNCTIONS = FunctionRegistry.getDefault();

    private Row rowOf(String key, SqlValue value) {
        return Row.eager(Map.of(FieldKey.of(key), value));
    }

    // ── Leaf evaluation ─��────────────────────────────────────────────────

    @Test
    void columnRef_returns_field_value() {
        Row row = rowOf("name", new SqlString("Alice"));
        SqlValue result = ExpressionEvaluator.evaluate(new ColumnRef("name"), row, FUNCTIONS);
        assertEquals(new SqlString("Alice"), result);
    }

    @Test
    void literalVal_returns_value_directly() {
        Row row = rowOf("x", SqlNull.INSTANCE);
        SqlValue result = ExpressionEvaluator.evaluate(new LiteralVal(SqlNumber.of(42)), row, FUNCTIONS);
        assertEquals(SqlNumber.of(42), result);
    }

    // ── Single function ──────────────────────────────────────────────────

    @Test
    void scalarFnCall_upper() {
        Row row = rowOf("name", new SqlString("alice"));
        Expression expr = new ScalarFnCall("upper", List.of(new ColumnRef("name")));
        SqlValue result = ExpressionEvaluator.evaluate(expr, row, FUNCTIONS);
        assertEquals(new SqlString("ALICE"), result);
    }

    // ── Nested scalar functions ─��────────────────────────────────────────

    @Test
    void nested_trim_nullif() {
        // TRIM(NULLIF(name, ''))
        Row row = rowOf("name", new SqlString("  hello  "));
        Expression expr = new ScalarFnCall(
                "trim",
                List.of(new ScalarFnCall("nullif", List.of(new ColumnRef("name"), new LiteralVal(new SqlString(""))))));
        SqlValue result = ExpressionEvaluator.evaluate(expr, row, FUNCTIONS);
        assertEquals(new SqlString("hello"), result);
    }

    @Test
    void nullif_returns_null_when_equal() {
        // NULLIF(rating, 0) where rating = 0
        Row row = rowOf("rating", SqlNumber.of(0));
        Expression expr = new ScalarFnCall("nullif", List.of(new ColumnRef("rating"), new LiteralVal(SqlNumber.of(0))));
        SqlValue result = ExpressionEvaluator.evaluate(expr, row, FUNCTIONS);
        assertEquals(SqlNull.INSTANCE, result);
    }

    @Test
    void deeply_nested_lpad_trim_nullif() {
        // LPAD(TRIM(NULLIF(name, '')), 10, '*')
        Row row = rowOf("name", new SqlString("  hi  "));
        Expression expr = new ScalarFnCall(
                "lpad",
                List.of(
                        new ScalarFnCall(
                                "trim",
                                List.of(new ScalarFnCall(
                                        "nullif", List.of(new ColumnRef("name"), new LiteralVal(new SqlString("")))))),
                        new LiteralVal(SqlNumber.of(10)),
                        new LiteralVal(new SqlString("*"))));
        SqlValue result = ExpressionEvaluator.evaluate(expr, row, FUNCTIONS);
        assertEquals(new SqlString("********hi"), result);
    }

    // ── Aggregate evaluation ───────���─────────────────────────────────────

    @Test
    void aggregate_avg_nullif_evaluates_inner_per_row_then_aggregates() {
        // AVG(NULLIF(salary, 0)) — should exclude zeros from average
        List<Row> group = List.of(
                rowOf("salary", SqlNumber.of(100)),
                rowOf("salary", SqlNumber.of(0)),
                rowOf("salary", SqlNumber.of(200)));
        Expression expr = new AggregateFnCall(
                "AVG", new ScalarFnCall("nullif", List.of(new ColumnRef("salary"), new LiteralVal(SqlNumber.of(0)))));
        SqlValue result = ExpressionEvaluator.evaluateAggregate(expr, group, FUNCTIONS);
        // AVG of [100, NULL, 200] — NULL excluded by AVG → (100 + 200) / 2 = 150
        assertEquals(150.0, ((SqlNumber) result).doubleValue(), 0.01);
    }

    @Test
    void round_avg_evaluates_post_aggregate_scalar() {
        // ROUND(AVG(salary), 0) — scalar wrapping aggregate
        List<Row> group = List.of(
                rowOf("salary", SqlNumber.of(100)),
                rowOf("salary", SqlNumber.of(200)),
                rowOf("salary", SqlNumber.of(200)));
        Expression expr = new ScalarFnCall(
                "round", List.of(new AggregateFnCall("AVG", new ColumnRef("salary")), new LiteralVal(SqlNumber.of(0))));
        SqlValue result = ExpressionEvaluator.evaluateAggregate(expr, group, FUNCTIONS);
        // AVG = 166.666..., ROUND(..., 0) = 167
        assertEquals(SqlNumber.of(167), result);
    }

    @Test
    void round_avg_nullif_full_chain() {
        // ROUND(AVG(NULLIF(salary, 0)), 2) — scalar(aggregate(scalar(col)))
        List<Row> group = List.of(
                rowOf("salary", SqlNumber.of(100)),
                rowOf("salary", SqlNumber.of(0)),
                rowOf("salary", SqlNumber.of(300)));
        Expression expr = new ScalarFnCall(
                "round",
                List.of(
                        new AggregateFnCall(
                                "AVG",
                                new ScalarFnCall(
                                        "nullif", List.of(new ColumnRef("salary"), new LiteralVal(SqlNumber.of(0))))),
                        new LiteralVal(SqlNumber.of(2))));
        SqlValue result = ExpressionEvaluator.evaluateAggregate(expr, group, FUNCTIONS);
        // NULLIF: [100, NULL, 300] → AVG: 200.0 → ROUND: 200.0
        assertEquals(200.0, ((SqlNumber) result).doubleValue(), 0.01);
    }

    @Test
    void count_asterisk() {
        List<Row> group =
                List.of(rowOf("x", SqlNumber.of(1)), rowOf("x", SqlNumber.of(2)), rowOf("x", SqlNumber.of(3)));
        Expression expr = new AggregateFnCall("COUNT", null); // COUNT(*)
        SqlValue result = ExpressionEvaluator.evaluateAggregate(expr, group, FUNCTIONS);
        assertEquals(SqlNumber.of(3), result);
    }

    // ── evaluateAggregate: ColumnRef and LiteralVal fallback ─────────────

    @Test
    void evaluateAggregate_columnRef_usesFirstRow() {
        List<Row> group =
                List.of(rowOf("dept", new SqlString("Engineering")), rowOf("dept", new SqlString("Engineering")));
        Expression expr = new ColumnRef("dept");
        SqlValue result = ExpressionEvaluator.evaluateAggregate(expr, group, FUNCTIONS);
        assertEquals(new SqlString("Engineering"), result);
    }

    @Test
    void evaluateAggregate_literalVal_returnsLiteral() {
        List<Row> group = List.of(rowOf("x", SqlNumber.of(1)));
        Expression expr = new LiteralVal(new SqlString("constant"));
        SqlValue result = ExpressionEvaluator.evaluateAggregate(expr, group, FUNCTIONS);
        assertEquals(new SqlString("constant"), result);
    }

    // ── evaluate: AggregateFnCall throws ─────────────────────────────────

    @Test
    void evaluate_aggregateInRowContext_throws() {
        Row row = rowOf("x", SqlNumber.of(1));
        Expression expr = new AggregateFnCall("COUNT", null);
        assertThrows(SQL4JsonExecutionException.class, () -> ExpressionEvaluator.evaluate(expr, row, FUNCTIONS));
    }

    // ── evaluate/evaluateAggregate: WindowFnCall throws ───────────────────

    @Test
    void evaluate_throws_for_window_function() {
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        Row row = Row.eager(Map.of());

        assertThrows(SQL4JsonExecutionException.class, () -> ExpressionEvaluator.evaluate(winExpr, row, FUNCTIONS));
    }

    @Test
    void evaluateAggregate_throws_for_window_function() {
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        List<Row> group = List.of(Row.eager(Map.of()));

        assertThrows(
                SQL4JsonExecutionException.class,
                () -> ExpressionEvaluator.evaluateAggregate(winExpr, group, FUNCTIONS));
    }
}
