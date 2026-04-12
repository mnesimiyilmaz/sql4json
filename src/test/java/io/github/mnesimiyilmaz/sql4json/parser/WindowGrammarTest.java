package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WindowGrammarTest {

    @Test
    void parse_row_number_with_order_by() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num FROM $r");

        assertEquals(2, query.selectedColumns().size());
        SelectColumnDef winCol = query.selectedColumns().get(1);
        assertTrue(winCol.containsWindow());
        assertEquals("row_num", winCol.alias());

        var winExpr = (Expression.WindowFnCall) winCol.expression();
        assertEquals("ROW_NUMBER", winExpr.name());
        assertTrue(winExpr.args().isEmpty());
        assertTrue(winExpr.spec().partitionBy().isEmpty());
        assertEquals(1, winExpr.spec().orderBy().size());
        assertEquals("DESC", winExpr.spec().orderBy().getFirst().direction());
    }

    @Test
    void parse_rank_with_partition_and_order() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk FROM $r");

        SelectColumnDef winCol = query.selectedColumns().get(1);
        var winExpr = (Expression.WindowFnCall) winCol.expression();
        assertEquals("RANK", winExpr.name());
        assertEquals(1, winExpr.spec().partitionBy().size());
        assertEquals(1, winExpr.spec().orderBy().size());
    }

    @Test
    void parse_sum_over_partition() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM $r");

        SelectColumnDef winCol = query.selectedColumns().get(1);
        var winExpr = (Expression.WindowFnCall) winCol.expression();
        assertEquals("SUM", winExpr.name());
        assertEquals(1, winExpr.args().size());
        assertInstanceOf(Expression.ColumnRef.class, winExpr.args().getFirst());
        assertEquals(1, winExpr.spec().partitionBy().size());
        assertTrue(winExpr.spec().orderBy().isEmpty());
    }

    @Test
    void parse_lag_with_offset() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, LAG(salary, 2) OVER (ORDER BY hire_date) AS prev_salary FROM $r");

        SelectColumnDef winCol = query.selectedColumns().get(1);
        var winExpr = (Expression.WindowFnCall) winCol.expression();
        assertEquals("LAG", winExpr.name());
        assertEquals(2, winExpr.args().size());
    }

    @Test
    void parse_count_star_over_partition() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, COUNT(*) OVER (PARTITION BY dept) AS dept_count FROM $r");

        SelectColumnDef winCol = query.selectedColumns().get(1);
        var winExpr = (Expression.WindowFnCall) winCol.expression();
        assertEquals("COUNT", winExpr.name());
        assertTrue(winExpr.args().isEmpty());
    }

    @Test
    void parse_empty_over_clause() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, COUNT(*) OVER () AS total FROM $r");

        SelectColumnDef winCol = query.selectedColumns().get(1);
        var winExpr = (Expression.WindowFnCall) winCol.expression();
        assertTrue(winExpr.spec().partitionBy().isEmpty());
        assertTrue(winExpr.spec().orderBy().isEmpty());
    }

    @Test
    void parse_multiple_window_functions() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, RANK() OVER (ORDER BY salary DESC) AS rnk, " +
                        "AVG(salary) OVER (PARTITION BY dept) AS dept_avg FROM $r");

        assertEquals(3, query.selectedColumns().size());
        assertTrue(query.selectedColumns().get(1).containsWindow());
        assertTrue(query.selectedColumns().get(2).containsWindow());
        assertTrue(query.containsWindowFunctions());
    }

    @Test
    void plain_aggregate_still_parses_without_over() {
        QueryDefinition query = QueryParser.parse(
                "SELECT dept, SUM(salary) AS total FROM $r GROUP BY dept");

        assertFalse(query.containsWindowFunctions());
        assertTrue(query.selectedColumns().get(1).containsAggregate());
    }

    @Test
    void keyword_as_field_name_still_works() {
        QueryDefinition query = QueryParser.parse("SELECT rank, lead FROM $r");

        assertFalse(query.containsWindowFunctions());
        assertEquals(2, query.selectedColumns().size());
        assertEquals("rank", query.selectedColumns().get(0).columnName());
        assertEquals("lead", query.selectedColumns().get(1).columnName());
    }

    @Test
    void window_function_tracks_referenced_fields() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, LAG(salary) OVER (PARTITION BY dept ORDER BY hire_date) AS prev FROM $r");

        assertTrue(query.referencedFields().contains("salary"));
        assertTrue(query.referencedFields().contains("dept"));
        assertTrue(query.referencedFields().contains("hire_date"));
        assertTrue(query.referencedFields().contains("name"));
    }

    @Test
    void parse_ntile() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, NTILE(4) OVER (ORDER BY salary DESC) AS quartile FROM $r");

        SelectColumnDef winCol = query.selectedColumns().get(1);
        var winExpr = (Expression.WindowFnCall) winCol.expression();
        assertEquals("NTILE", winExpr.name());
        assertEquals(1, winExpr.args().size());
    }

    @Test
    void parse_multi_column_partition_by() {
        QueryDefinition query = QueryParser.parse(
                "SELECT name, ROW_NUMBER() OVER (PARTITION BY dept, team ORDER BY salary) AS rn FROM $r");

        SelectColumnDef winCol = query.selectedColumns().get(1);
        var winExpr = (Expression.WindowFnCall) winCol.expression();
        assertEquals(2, winExpr.spec().partitionBy().size());
    }
}
