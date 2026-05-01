package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQL-level smoke + edge-case integration test for every registered function.
 *
 * <p>Each function is exercised through real SQL ({@link SQL4Json#query(String, String)})
 * against a single shared JSON fixture, with assertions on the JSON output. Distinct from
 * {@code FunctionUnitTest} which calls the registry directly at the Java level — this class
 * verifies the parser→executor→serializer pipeline as a whole.
 */
class FunctionIntegrationTest {

    private static final String FIXTURE = """
            [
              {"name":"Alice","age":30,"dept":"Engineering","salary":90000,"hired":"2020-01-15","active":true},
              {"name":"Bob","age":25,"dept":"Marketing","salary":70000,"hired":"2019-06-01","active":false},
              {"name":"Charlie","age":35,"dept":"Engineering","salary":85000,"hired":"2021-03-10","active":true},
              {"name":"  Diana  ","age":40,"dept":"Marketing","salary":120000,"hired":"2018-11-20","active":true},
              {"name":null,"age":null,"dept":null,"salary":null,"hired":null,"active":null}
            ]""";

    private static String run(String sql) {
        return SQL4Json.query(sql, FIXTURE);
    }

    // ── String functions ────────────────────────────────────────────────

    @Nested
    class StringFunctions {

        @Test
        void lower_happy() {
            assertEquals(
                    "[{\"s\":\"alice\"},{\"s\":\"bob\"},{\"s\":\"charlie\"},{\"s\":\"  diana  \"},{\"s\":null}]",
                    run("SELECT LOWER(name) AS s FROM $r"));
        }

        @Test
        void upper_happy() {
            assertEquals(
                    "[{\"s\":\"ALICE\"},{\"s\":\"BOB\"},{\"s\":\"CHARLIE\"},{\"s\":\"  DIANA  \"},{\"s\":null}]",
                    run("SELECT UPPER(name) AS s FROM $r"));
        }

        @Test
        void lower_coerces_number() {
            assertEquals(
                    "[{\"s\":\"90000\"},{\"s\":\"70000\"},{\"s\":\"85000\"},{\"s\":\"120000\"},{\"s\":null}]",
                    run("SELECT LOWER(salary) AS s FROM $r"));
        }

        @Test
        void upper_coerces_boolean() {
            assertEquals(
                    "[{\"s\":\"TRUE\"},{\"s\":\"FALSE\"},{\"s\":\"TRUE\"},{\"s\":\"TRUE\"},{\"s\":null}]",
                    run("SELECT UPPER(active) AS s FROM $r"));
        }

        @Test
        void substring_happy() {
            // start=2 (1-based), length=2 — "Alice"→"li", "Bob"→"ob", "Charlie"→"ha", "  Diana  "→" D"
            assertEquals(
                    "[{\"s\":\"li\"},{\"s\":\"ob\"},{\"s\":\"ha\"},{\"s\":\" D\"},{\"s\":null}]",
                    run("SELECT SUBSTRING(name, 2, 2) AS s FROM $r"));
        }

        @Test
        void substring_no_length_takes_to_end() {
            assertEquals(
                    "[{\"s\":\"lice\"},{\"s\":\"ob\"},{\"s\":\"harlie\"},{\"s\":\" Diana  \"},{\"s\":null}]",
                    run("SELECT SUBSTRING(name, 2) AS s FROM $r"));
        }

        @Test
        void substring_coerces_number_extracts_prefix() {
            assertEquals(
                    "[{\"s\":\"900\"},{\"s\":\"700\"},{\"s\":\"850\"},{\"s\":\"120\"},{\"s\":null}]",
                    run("SELECT SUBSTRING(salary, 1, 3) AS s FROM $r"));
        }

        @Test
        void substring_extracts_year_from_date_string() {
            assertEquals(
                    "[{\"s\":\"2020\"},{\"s\":\"2019\"},{\"s\":\"2021\"},{\"s\":\"2018\"},{\"s\":null}]",
                    run("SELECT SUBSTRING(hired, 1, 4) AS s FROM $r"));
        }

        @Test
        void substring_start_beyond_length_returns_empty() {
            assertEquals(
                    "[{\"s\":\"\"},{\"s\":\"\"},{\"s\":\"\"},{\"s\":\"\"},{\"s\":null}]",
                    run("SELECT SUBSTRING(name, 100, 3) AS s FROM $r"));
        }

        @Test
        void trim_strips_whitespace() {
            String result = run("SELECT TRIM(name) AS s FROM $r");
            assertTrue(result.contains("\"s\":\"Diana\""), result);
            assertTrue(result.contains("\"s\":\"Alice\""), result);
            assertTrue(result.contains("\"s\":null"), result);
        }

        @Test
        void length_happy() {
            assertEquals(
                    "[{\"s\":5},{\"s\":3},{\"s\":7},{\"s\":9},{\"s\":null}]",
                    run("SELECT LENGTH(name) AS s FROM $r"));
        }

        @Test
        void length_coerces_number() {
            assertEquals(
                    "[{\"s\":5},{\"s\":5},{\"s\":5},{\"s\":6},{\"s\":null}]",
                    run("SELECT LENGTH(salary) AS s FROM $r"));
        }

        @Test
        void replace_happy() {
            assertEquals(
                    "[{\"s\":\"AlIce\"},{\"s\":\"Bob\"},{\"s\":\"CharlIe\"},{\"s\":\"  DIana  \"},{\"s\":null}]",
                    run("SELECT REPLACE(name, 'i', 'I') AS s FROM $r"));
        }

        @Test
        void left_happy() {
            assertEquals(
                    "[{\"s\":\"Al\"},{\"s\":\"Bo\"},{\"s\":\"Ch\"},{\"s\":\"  \"},{\"s\":null}]",
                    run("SELECT LEFT(name, 2) AS s FROM $r"));
        }

        @Test
        void right_happy() {
            assertEquals(
                    "[{\"s\":\"ce\"},{\"s\":\"ob\"},{\"s\":\"ie\"},{\"s\":\"  \"},{\"s\":null}]",
                    run("SELECT RIGHT(name, 2) AS s FROM $r"));
        }

        @Test
        void lpad_happy() {
            // "  Diana  " is 9 chars; padded by 1 to length 10 → "*  Diana  "
            assertEquals(
                    "[{\"s\":\"*****Alice\"},{\"s\":\"*******Bob\"},{\"s\":\"***Charlie\"},{\"s\":\"*  Diana  \"},{\"s\":null}]",
                    run("SELECT LPAD(name, 10, '*') AS s FROM $r"));
        }

        @Test
        void lpad_coerces_number_input() {
            assertEquals(
                    "[{\"s\":\"0000090000\"},{\"s\":\"0000070000\"},{\"s\":\"0000085000\"},{\"s\":\"0000120000\"},{\"s\":null}]",
                    run("SELECT LPAD(salary, 10, '0') AS s FROM $r"));
        }

        @Test
        void rpad_happy() {
            assertEquals(
                    "[{\"s\":\"Alice*****\"},{\"s\":\"Bob*******\"},{\"s\":\"Charlie***\"},{\"s\":\"  Diana  *\"},{\"s\":null}]",
                    run("SELECT RPAD(name, 10, '*') AS s FROM $r"));
        }

        @Test
        void reverse_happy() {
            assertEquals(
                    "[{\"s\":\"ecilA\"},{\"s\":\"boB\"},{\"s\":\"eilrahC\"},{\"s\":\"  anaiD  \"},{\"s\":null}]",
                    run("SELECT REVERSE(name) AS s FROM $r"));
        }

        @Test
        void position_happy() {
            // POSITION(substr, str) → 1-based index, 0 if not found
            // 'a' positions: Alice→0 (uppercase A), Bob→0, Charlie→3, "  Diana  "→5, null→null
            assertEquals(
                    "[{\"s\":0},{\"s\":0},{\"s\":3},{\"s\":5},{\"s\":null}]",
                    run("SELECT POSITION('a', name) AS s FROM $r"));
        }

        @Test
        void concat_field_plus_literal_separator() {
            assertEquals(
                    "[{\"s\":\"Alice - Engineering\"},{\"s\":\"Bob - Marketing\"},{\"s\":\"Charlie - Engineering\"},{\"s\":\"  Diana   - Marketing\"},{\"s\":null}]",
                    run("SELECT CONCAT(name, ' - ', dept) AS s FROM $r"));
        }
    }

    // ── Math functions ──────────────────────────────────────────────────

    @Nested
    class MathFunctions {

        @Test
        void abs_happy() {
            assertEquals(
                    "[{\"s\":30},{\"s\":25},{\"s\":35},{\"s\":40},{\"s\":null}]",
                    run("SELECT ABS(age) AS s FROM $r"));
        }

        @Test
        void round_no_decimals_returns_int() {
            assertEquals(
                    "[{\"s\":3},{\"s\":3},{\"s\":3},{\"s\":3},{\"s\":3}]",
                    run("SELECT ROUND(3.14) AS s FROM $r"));
        }

        @Test
        void round_two_decimals_preserved() {
            assertEquals(
                    "[{\"s\":3.14},{\"s\":3.14},{\"s\":3.14},{\"s\":3.14},{\"s\":3.14}]",
                    run("SELECT ROUND(3.14159, 2) AS s FROM $r"));
        }

        @Test
        void ceil_happy() {
            assertEquals(
                    "[{\"s\":4},{\"s\":4},{\"s\":4},{\"s\":4},{\"s\":4}]",
                    run("SELECT CEIL(3.14) AS s FROM $r"));
        }

        @Test
        void floor_happy() {
            assertEquals(
                    "[{\"s\":3},{\"s\":3},{\"s\":3},{\"s\":3},{\"s\":3}]",
                    run("SELECT FLOOR(3.99) AS s FROM $r"));
        }

        @Test
        void mod_happy() {
            // age % 5 = 0 for 30, 25, 35, 40; null for null row
            assertEquals(
                    "[{\"s\":0},{\"s\":0},{\"s\":0},{\"s\":0},{\"s\":null}]",
                    run("SELECT MOD(age, 5) AS s FROM $r"));
        }

        @Test
        void mod_zero_divisor_returns_null() {
            assertEquals(
                    "[{\"s\":null},{\"s\":null},{\"s\":null},{\"s\":null},{\"s\":null}]",
                    run("SELECT MOD(age, 0) AS s FROM $r"));
        }

        @Test
        void power_happy() {
            assertEquals(
                    "[{\"s\":900},{\"s\":625},{\"s\":1225},{\"s\":1600},{\"s\":null}]",
                    run("SELECT POWER(age, 2) AS s FROM $r"));
        }

        @Test
        void sqrt_returns_exact_for_perfect_squares() {
            // age=25 → 5; age=null → null. Other ages have non-integer roots.
            String result = run("SELECT SQRT(age) AS s FROM $r");
            assertTrue(result.contains("\"s\":5}"), result);
            assertTrue(result.contains("\"s\":null"), result);
        }

        @Test
        void sqrt_negative_returns_null() {
            assertEquals(
                    "[{\"s\":null},{\"s\":null},{\"s\":null},{\"s\":null},{\"s\":null}]",
                    run("SELECT SQRT(-1) AS s FROM $r"));
        }

        @Test
        void sign_happy() {
            // All positive ages → 1; null → null
            assertEquals(
                    "[{\"s\":1},{\"s\":1},{\"s\":1},{\"s\":1},{\"s\":null}]",
                    run("SELECT SIGN(age) AS s FROM $r"));
        }
    }

    // ── Date functions ──────────────────────────────────────────────────

    @Nested
    class DateFunctions {

        @Test
        void to_date_then_year() {
            assertEquals(
                    "[{\"s\":2020},{\"s\":2019},{\"s\":2021},{\"s\":2018},{\"s\":null}]",
                    run("SELECT YEAR(TO_DATE(hired)) AS s FROM $r"));
        }

        @Test
        void month_happy() {
            assertEquals(
                    "[{\"s\":1},{\"s\":6},{\"s\":3},{\"s\":11},{\"s\":null}]",
                    run("SELECT MONTH(TO_DATE(hired)) AS s FROM $r"));
        }

        @Test
        void day_happy() {
            assertEquals(
                    "[{\"s\":15},{\"s\":1},{\"s\":10},{\"s\":20},{\"s\":null}]",
                    run("SELECT DAY(TO_DATE(hired)) AS s FROM $r"));
        }

        @Test
        void date_add_days_within_year() {
            // adding 5 days to all hired dates stays in the same year
            assertEquals(
                    "[{\"s\":2020},{\"s\":2019},{\"s\":2021},{\"s\":2018},{\"s\":null}]",
                    run("SELECT YEAR(DATE_ADD(TO_DATE(hired), 5, 'DAY')) AS s FROM $r"));
        }

        @Test
        void date_diff_against_self_is_zero() {
            assertEquals(
                    "[{\"s\":0},{\"s\":0},{\"s\":0},{\"s\":0},{\"s\":null}]",
                    run("SELECT DATE_DIFF(TO_DATE(hired), TO_DATE(hired), 'DAY') AS s FROM $r"));
        }

        @Test
        void now_filters_in_where() {
            // NOW() is always > 1970 — every non-null name passes through
            String result = run("SELECT name FROM $r WHERE NOW() > TO_DATE('1970-01-01')");
            assertTrue(result.contains("\"name\":\"Alice\""), result);
            assertTrue(result.contains("\"name\":\"Bob\""), result);
        }
    }

    // ── Conversion functions ────────────────────────────────────────────

    @Nested
    class Conversion {

        @Test
        void cast_to_string() {
            assertEquals(
                    "[{\"s\":\"30\"},{\"s\":\"25\"},{\"s\":\"35\"},{\"s\":\"40\"},{\"s\":null}]",
                    run("SELECT CAST(age AS STRING) AS s FROM $r"));
        }

        @Test
        void cast_to_integer_truncates() {
            // CAST INTEGER currently produces double-typed truncated value (3.0); see castValue()
            assertEquals(
                    "[{\"s\":3.0},{\"s\":3.0},{\"s\":3.0},{\"s\":3.0},{\"s\":3.0}]",
                    run("SELECT CAST(3.7 AS INTEGER) AS s FROM $r"));
        }

        @Test
        void nullif_match_returns_null() {
            assertEquals(
                    "[{\"s\":\"Alice\"},{\"s\":null},{\"s\":\"Charlie\"},{\"s\":\"  Diana  \"},{\"s\":null}]",
                    run("SELECT NULLIF(name, 'Bob') AS s FROM $r"));
        }

        @Test
        void coalesce_picks_first_non_null() {
            assertEquals(
                    "[{\"s\":\"Alice\"},{\"s\":\"Bob\"},{\"s\":\"Charlie\"},{\"s\":\"  Diana  \"},{\"s\":\"unknown\"}]",
                    run("SELECT COALESCE(name, 'unknown') AS s FROM $r"));
        }

        @Test
        void cast_boolean_to_string() {
            String result = run("SELECT CAST(active AS STRING) AS s FROM $r");
            assertTrue(result.contains("\"s\":\"true\""), result);
            assertTrue(result.contains("\"s\":\"false\""), result);
        }

        @Test
        void cast_date_to_string() {
            String result = run("SELECT CAST(TO_DATE(hired) AS STRING) AS s FROM $r WHERE name = 'Alice'");
            assertTrue(result.contains("\"s\":\"2020-01-15\""), result);
        }

        @Test
        void cast_null_to_string_returns_null() {
            String result = run("SELECT CAST(name AS STRING) AS s FROM $r WHERE age IS NULL");
            assertTrue(result.contains("\"s\":null"), result);
        }

        @Test
        void cast_string_to_number() {
            String result = run("SELECT CAST('42.5' AS NUMBER) AS s FROM $r WHERE name = 'Alice'");
            assertTrue(result.contains("\"s\":42.5"), result);
        }

        @Test
        void cast_invalid_string_to_number_throws() {
            assertThrows(io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException.class,
                    () -> run("SELECT CAST('not a number' AS NUMBER) AS s FROM $r"));
        }

        @Test
        void cast_boolean_to_number() {
            String result = run("SELECT CAST(active AS NUMBER) AS s FROM $r WHERE name = 'Alice'");
            assertTrue(result.contains("\"s\":1"), result);
        }

        @Test
        void cast_date_to_number_returns_epoch_day() {
            String result = run("SELECT CAST(TO_DATE(hired) AS NUMBER) AS s FROM $r WHERE name = 'Alice'");
            // 2020-01-15 → epoch day 18276
            assertTrue(result.contains("\"s\":18276"), result);
        }

        @Test
        void cast_string_to_boolean() {
            String result = run("SELECT CAST('true' AS BOOLEAN) AS s FROM $r WHERE name = 'Alice'");
            assertTrue(result.contains("\"s\":true"), result);
        }

        @Test
        void cast_number_to_boolean() {
            String result = run("SELECT CAST(1 AS BOOLEAN) AS s FROM $r WHERE name = 'Alice'");
            assertTrue(result.contains("\"s\":true"), result);
        }

        @Test
        void cast_invalid_type_to_boolean_throws() {
            // Date cannot be cast to boolean
            assertThrows(io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException.class,
                    () -> run("SELECT CAST(TO_DATE(hired) AS BOOLEAN) AS s FROM $r WHERE name = 'Alice'"));
        }

        @Test
        void cast_string_to_date_invalid_throws() {
            assertThrows(io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException.class,
                    () -> run("SELECT CAST('not a date' AS DATE) AS s FROM $r"));
        }

        @Test
        void cast_dateTime_to_date() {
            String result = run("SELECT CAST(CAST('2020-01-15T10:30:00' AS DATETIME) AS DATE) AS s FROM $r WHERE name = 'Alice'");
            assertTrue(result.contains("\"s\":\"2020-01-15\""), result);
        }

        @Test
        void cast_date_to_dateTime() {
            String result = run("SELECT CAST(TO_DATE(hired) AS DATETIME) AS s FROM $r WHERE name = 'Alice'");
            assertTrue(result.contains("\"s\":\"2020-01-15T00:00\""), result);
        }

        @Test
        void cast_string_to_dateTime_invalid_throws() {
            assertThrows(io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException.class,
                    () -> run("SELECT CAST('bad' AS DATETIME) AS s FROM $r"));
        }
    }

    // ── Aggregate functions ─────────────────────────────────────────────

    @Nested
    class Aggregate {
        // Note: this engine requires GROUP BY for aggregate queries; "SELECT COUNT(*) FROM $r"
        // without GROUP BY is unsupported (per QueryExecutor logic). All aggregate tests
        // therefore exercise the function via GROUP BY.

        @Test
        void count_star_per_dept() {
            // 5 rows: Engineering=2, Marketing=2, null=1
            String result = run("SELECT dept, COUNT(*) AS c FROM $r GROUP BY dept");
            assertTrue(result.contains("\"dept\":\"Engineering\",\"c\":2"), result);
            assertTrue(result.contains("\"dept\":\"Marketing\",\"c\":2"), result);
            assertTrue(result.contains("\"dept\":null,\"c\":1"), result);
        }

        @Test
        void count_col_skips_nulls_per_dept() {
            // COUNT(name) skips null names per SQL standard; null-dept group has 1 row
            // with null name, so count is 0 for that group.
            String result = run("SELECT dept, COUNT(name) AS c FROM $r GROUP BY dept");
            assertTrue(result.contains("\"dept\":null,\"c\":0"), result);
        }

        @Test
        void sum_per_dept() {
            // Engineering: 90000 + 85000 = 175000; Marketing: 70000 + 120000 = 190000
            String result = run("SELECT dept, SUM(salary) AS s FROM $r GROUP BY dept");
            assertTrue(result.contains("\"s\":175000.0"), result);
            assertTrue(result.contains("\"s\":190000.0"), result);
        }

        @Test
        void avg_per_dept() {
            // Engineering: (90000+85000)/2 = 87500; Marketing: (70000+120000)/2 = 95000
            String result = run("SELECT dept, AVG(salary) AS a FROM $r GROUP BY dept");
            assertTrue(result.contains("\"a\":87500.0"), result);
            assertTrue(result.contains("\"a\":95000.0"), result);
        }

        @Test
        void min_max_per_dept() {
            String result = run(
                    "SELECT dept, MIN(salary) AS lo, MAX(salary) AS hi FROM $r GROUP BY dept");
            assertTrue(result.contains("\"lo\":85000,\"hi\":90000"), result);   // Engineering
            assertTrue(result.contains("\"lo\":70000,\"hi\":120000"), result);  // Marketing
        }

        @Test
        void having_filters_groups() {
            String result = run(
                    "SELECT dept, COUNT(*) AS c FROM $r GROUP BY dept HAVING c > 1");
            assertTrue(result.contains("\"dept\":\"Engineering\""), result);
            assertTrue(result.contains("\"dept\":\"Marketing\""), result);
            // null-dept group has c=1 — must be filtered out
            assertTrue(!result.contains("\"dept\":null"), result);
        }
    }

    // ── Window functions ────────────────────────────────────────────────

    @Nested
    class Window {

        @Test
        void row_number_with_over() {
            String result = run("SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM $r");
            assertTrue(result.contains("\"rn\":1"), result);
            assertTrue(result.contains("\"rn\":5"), result);
        }

        @Test
        void rank_with_over() {
            String result = run("SELECT name, RANK() OVER (ORDER BY salary DESC) AS r FROM $r");
            assertTrue(result.contains("\"r\":1"), result);
        }

        @Test
        void dense_rank_with_over() {
            String result = run("SELECT name, DENSE_RANK() OVER (ORDER BY salary DESC) AS r FROM $r");
            assertTrue(result.contains("\"r\":1"), result);
        }

        @Test
        void aggregate_as_window() {
            String result = run("SELECT name, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM $r");
            assertTrue(result.contains("\"dept_total\""), result);
        }
    }

    // ── Literal / derived columns ───────────────────────────────────────

    @Nested
    class LiteralAndDerivedColumns {

        @Test
        void string_literal_as_column() {
            assertEquals(
                    "[{\"x\":\"hello\"},{\"x\":\"hello\"},{\"x\":\"hello\"},{\"x\":\"hello\"},{\"x\":\"hello\"}]",
                    run("SELECT 'hello' AS x FROM $r"));
        }

        @Test
        void whole_number_literal_serializes_as_int() {
            // 42 stays as int, not 42.0
            assertEquals(
                    "[{\"x\":42},{\"x\":42},{\"x\":42},{\"x\":42},{\"x\":42}]",
                    run("SELECT 42 AS x FROM $r"));
        }

        @Test
        void decimal_literal_preserved() {
            assertEquals(
                    "[{\"x\":3.14},{\"x\":3.14},{\"x\":3.14},{\"x\":3.14},{\"x\":3.14}]",
                    run("SELECT 3.14 AS x FROM $r"));
        }

        @Test
        void boolean_literal_as_column() {
            assertEquals(
                    "[{\"x\":true},{\"x\":true},{\"x\":true},{\"x\":true},{\"x\":true}]",
                    run("SELECT TRUE AS x FROM $r"));
        }

        @Test
        void function_with_only_literal_args() {
            assertEquals(
                    "[{\"x\":3.14},{\"x\":3.14},{\"x\":3.14},{\"x\":3.14},{\"x\":3.14}]",
                    run("SELECT ROUND(3.14159, 2) AS x FROM $r"));
        }

        @Test
        void concat_field_plus_literal_full_name() {
            String result = run("SELECT CONCAT(name, ' (', dept, ')') AS x FROM $r");
            assertTrue(result.contains("\"x\":\"Alice (Engineering)\""), result);
            assertTrue(result.contains("\"x\":\"Bob (Marketing)\""), result);
        }

        @Test
        void nested_function_calls() {
            String result = run("SELECT CONCAT(UPPER(name), '!') AS x FROM $r");
            assertTrue(result.contains("\"x\":\"ALICE!\""), result);
            assertTrue(result.contains("\"x\":\"BOB!\""), result);
        }

        @Test
        void mixed_field_and_literal_columns() {
            String result = run("SELECT name, 'Hello' AS greeting FROM $r");
            assertTrue(result.contains("\"name\":\"Alice\""), result);
            assertTrue(result.contains("\"greeting\":\"Hello\""), result);
        }
    }

    // ── Error cases ─────────────────────────────────────────────────────

    @Nested
    class ErrorCases {

        @Test
        void row_number_without_over_throws_parse_error() {
            var ex = assertThrows(SQL4JsonParseException.class,
                    () -> run("SELECT ROW_NUMBER() AS rn FROM $r"));
            assertTrue(ex.getMessage().contains("ROW_NUMBER must be used with OVER"),
                    "actual: " + ex.getMessage());
        }

        @Test
        void rank_without_over_throws_parse_error() {
            var ex = assertThrows(SQL4JsonParseException.class,
                    () -> run("SELECT RANK() AS r FROM $r"));
            assertTrue(ex.getMessage().contains("RANK must be used with OVER"),
                    "actual: " + ex.getMessage());
        }

        @Test
        void dense_rank_without_over_throws_parse_error() {
            var ex = assertThrows(SQL4JsonParseException.class,
                    () -> run("SELECT DENSE_RANK() AS r FROM $r"));
            assertTrue(ex.getMessage().contains("DENSE_RANK must be used with OVER"),
                    "actual: " + ex.getMessage());
        }

        @Test
        void ntile_without_over_throws_parse_error() {
            var ex = assertThrows(SQL4JsonParseException.class,
                    () -> run("SELECT NTILE(4) AS r FROM $r"));
            assertTrue(ex.getMessage().contains("NTILE must be used with OVER"),
                    "actual: " + ex.getMessage());
        }

        @Test
        void lag_without_over_throws_parse_error() {
            var ex = assertThrows(SQL4JsonParseException.class,
                    () -> run("SELECT LAG(name) AS r FROM $r"));
            assertTrue(ex.getMessage().contains("LAG must be used with OVER"),
                    "actual: " + ex.getMessage());
        }

        @Test
        void lead_without_over_throws_parse_error() {
            var ex = assertThrows(SQL4JsonParseException.class,
                    () -> run("SELECT LEAD(name) AS r FROM $r"));
            assertTrue(ex.getMessage().contains("LEAD must be used with OVER"),
                    "actual: " + ex.getMessage());
        }
    }
}
