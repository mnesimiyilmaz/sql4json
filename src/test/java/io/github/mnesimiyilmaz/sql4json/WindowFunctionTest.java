package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WindowFunctionTest {

    static final String EMPLOYEES_JSON = """
            [
              {"name": "Alice", "dept": "Engineering", "salary": 90000, "hire_date": "2020-01-15"},
              {"name": "Bob", "dept": "Engineering", "salary": 80000, "hire_date": "2019-06-01"},
              {"name": "Charlie", "dept": "Marketing", "salary": 70000, "hire_date": "2021-03-10"},
              {"name": "Diana", "dept": "Marketing", "salary": 85000, "hire_date": "2018-11-20"},
              {"name": "Eve", "dept": "Engineering", "salary": 95000, "hire_date": "2022-07-01"}
            ]""";

    // ── ROW_NUMBER ──────────────────────────────────────────────────────

    @Test
    void row_number_global_ranking() {
        String result = SQL4Json.query(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"row_num\":1"));
        assertTrue(result.contains("\"row_num\":5"));
    }

    @Test
    void row_number_partitioned_by_dept() {
        String result = SQL4Json.query(
                "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"dept_rank\":1"));
        assertTrue(result.contains("\"dept_rank\":2"));
        assertTrue(result.contains("\"dept_rank\":3"));
    }

    // ── RANK / DENSE_RANK ───────────────────────────────────────────────

    @Test
    void rank_with_ties() {
        String json = """
                [
                  {"name": "A", "score": 100},
                  {"name": "B", "score": 100},
                  {"name": "C", "score": 90},
                  {"name": "D", "score": 80}
                ]""";
        String result = SQL4Json.query(
                "SELECT name, RANK() OVER (ORDER BY score DESC) AS rnk FROM $r", json);
        assertTrue(result.contains("\"rnk\":1"));
        assertTrue(result.contains("\"rnk\":3"));
        assertTrue(result.contains("\"rnk\":4"));
        assertFalse(result.contains("\"rnk\":2"));
    }

    @Test
    void dense_rank_no_gaps() {
        String json = """
                [
                  {"name": "A", "score": 100},
                  {"name": "B", "score": 100},
                  {"name": "C", "score": 90},
                  {"name": "D", "score": 80}
                ]""";
        String result = SQL4Json.query(
                "SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM $r", json);
        assertTrue(result.contains("\"drnk\":1"));
        assertTrue(result.contains("\"drnk\":2"));
        assertTrue(result.contains("\"drnk\":3"));
    }

    // ── NTILE ───────────────────────────────────────────────────────────

    @Test
    void ntile_quartiles() {
        String result = SQL4Json.query(
                "SELECT name, NTILE(4) OVER (ORDER BY salary DESC) AS quartile FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"quartile\":1"));
        assertTrue(result.contains("\"quartile\":2"));
        assertTrue(result.contains("\"quartile\":3"));
        assertTrue(result.contains("\"quartile\":4"));
    }

    // ── LAG / LEAD ──────────────────────────────────────────────────────

    @Test
    void lag_default_offset() {
        String result = SQL4Json.query(
                "SELECT name, salary, LAG(salary) OVER (ORDER BY salary ASC) AS prev_salary FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"prev_salary\":null"));
    }

    @Test
    void lag_custom_offset() {
        String result = SQL4Json.query(
                "SELECT name, LAG(salary, 2) OVER (ORDER BY salary ASC) AS prev2 FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"prev2\":null"));
    }

    @Test
    void lead_default_offset() {
        String result = SQL4Json.query(
                "SELECT name, salary, LEAD(salary) OVER (ORDER BY salary ASC) AS next_salary FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"next_salary\":null"));
    }

    // ── Aggregate windows ───────────────────────────────────────────────

    @Test
    void sum_over_partition() {
        String result = SQL4Json.query(
                "SELECT name, dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"dept_total\":265000"));
        assertTrue(result.contains("\"dept_total\":155000"));
        JsonValue jv = SQL4Json.queryAsJsonValue(
                "SELECT name, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM $r",
                EMPLOYEES_JSON);
        assertEquals(5, jv.asArray().orElseThrow().size());
    }

    @Test
    void avg_over_partition() {
        String result = SQL4Json.query(
                "SELECT name, dept, AVG(salary) OVER (PARTITION BY dept) AS dept_avg FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"dept_avg\":"));
    }

    @Test
    void count_star_over_partition() {
        String result = SQL4Json.query(
                "SELECT name, dept, COUNT(*) OVER (PARTITION BY dept) AS dept_count FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"dept_count\":3"));
        assertTrue(result.contains("\"dept_count\":2"));
    }

    @Test
    void min_max_over_partition() {
        String result = SQL4Json.query(
                "SELECT name, dept, MIN(salary) OVER (PARTITION BY dept) AS dept_min, " +
                        "MAX(salary) OVER (PARTITION BY dept) AS dept_max FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"dept_min\":80000"));
        assertTrue(result.contains("\"dept_max\":95000"));
        assertTrue(result.contains("\"dept_min\":70000"));
        assertTrue(result.contains("\"dept_max\":85000"));
    }

    @Test
    void count_over_empty_partition_clause() {
        String result = SQL4Json.query(
                "SELECT name, COUNT(*) OVER () AS total FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"total\":5"));
    }

    // ── Mixed queries ───────────────────────────────────────────────────

    @Test
    void window_with_where_filter() {
        JsonValue jv = SQL4Json.queryAsJsonValue(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rnk " +
                        "FROM $r WHERE dept = 'Engineering'",
                EMPLOYEES_JSON);
        assertEquals(3, jv.asArray().orElseThrow().size());
    }

    @Test
    void window_with_order_by_and_limit() {
        JsonValue jv = SQL4Json.queryAsJsonValue(
                "SELECT name, salary, RANK() OVER (ORDER BY salary DESC) AS rnk " +
                        "FROM $r ORDER BY rnk LIMIT 3",
                EMPLOYEES_JSON);
        assertEquals(3, jv.asArray().orElseThrow().size());
    }

    @Test
    void multiple_window_functions_different_specs() {
        String result = SQL4Json.query(
                "SELECT name, dept, salary, " +
                        "RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank, " +
                        "RANK() OVER (ORDER BY salary DESC) AS global_rank, " +
                        "AVG(salary) OVER (PARTITION BY dept) AS dept_avg " +
                        "FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"dept_rank\":"));
        assertTrue(result.contains("\"global_rank\":"));
        assertTrue(result.contains("\"dept_avg\":"));
    }

    // ── Prepared query / Engine ─────────────────────────────────────────

    @Test
    void prepared_query_with_window_function() {
        PreparedQuery pq = SQL4Json.prepare(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rnk FROM $r");
        String result = pq.execute(EMPLOYEES_JSON);
        assertTrue(result.contains("\"rnk\":1"));
    }

    @Test
    void engine_with_window_function() {
        SQL4JsonEngine engine = SQL4Json.engine().data(EMPLOYEES_JSON).build();
        String result = engine.query(
                "SELECT name, RANK() OVER (ORDER BY salary DESC) AS rnk FROM $r");
        assertTrue(result.contains("\"rnk\":1"));
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    @Test
    void single_row_window_function() {
        String json = "[{\"name\": \"Solo\", \"val\": 42}]";
        String result = SQL4Json.query(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY val) AS rn, " +
                        "LAG(val) OVER (ORDER BY val) AS prev, " +
                        "LEAD(val) OVER (ORDER BY val) AS next FROM $r", json);
        assertTrue(result.contains("\"rn\":1"));
        assertTrue(result.contains("\"prev\":null"));
        assertTrue(result.contains("\"next\":null"));
    }

    @Test
    void window_function_with_nulls() {
        String json = """
                [
                  {"name": "A", "dept": "X", "val": 10},
                  {"name": "B", "dept": "X", "val": null},
                  {"name": "C", "dept": "X", "val": 30}
                ]""";
        String result = SQL4Json.query(
                "SELECT name, SUM(val) OVER (PARTITION BY dept) AS total FROM $r", json);
        assertTrue(result.contains("\"total\":"));
    }

    @Test
    void partition_by_only_no_order_by() {
        String result = SQL4Json.query(
                "SELECT name, dept, COUNT(*) OVER (PARTITION BY dept) AS dept_size FROM $r",
                EMPLOYEES_JSON);
        assertTrue(result.contains("\"dept_size\":3"));
        assertTrue(result.contains("\"dept_size\":2"));
    }

    @Test
    void window_function_in_where_throws_error() {
        assertThrows(Exception.class, () -> SQL4Json.query(
                "SELECT name FROM $r WHERE ROW_NUMBER() OVER (ORDER BY salary) = 1",
                EMPLOYEES_JSON));
    }

    @Test
    void keyword_field_names_still_accessible() {
        String json = """
                [
                  {"rank": 1, "lead": "Alice", "partition": "A"},
                  {"rank": 2, "lead": "Bob", "partition": "B"}
                ]""";
        String result = SQL4Json.query("SELECT rank, lead, partition FROM $r", json);
        assertTrue(result.contains("\"rank\":1"));
        assertTrue(result.contains("\"lead\":\"Alice\""));
        assertTrue(result.contains("\"partition\":\"A\""));
    }
}
