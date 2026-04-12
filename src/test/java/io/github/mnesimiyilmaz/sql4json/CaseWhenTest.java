package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CaseWhenTest {

    private static final String DATA = """
            [
              {"id": 1, "name": "Alice", "dept": "eng", "salary": 90000, "status": "active", "hire_date": "2020-03-15"},
              {"id": 2, "name": "Bob", "dept": "sales", "salary": 50000, "status": "inactive", "hire_date": "2018-06-01"},
              {"id": 3, "name": "Carol", "dept": "eng", "salary": 110000, "status": "active", "hire_date": "2019-11-20"},
              {"id": 4, "name": "Dave", "dept": "hr", "salary": 60000, "status": null, "hire_date": "2021-01-10"},
              {"id": 5, "name": "Eve", "dept": "eng", "salary": 75000, "status": "active", "hire_date": "2023-07-01"}
            ]
            """;

    // ── helpers ──────────────────────────────────────────────────────────

    private static List<JsonValue> arr(JsonValue v) {
        return v.asArray().orElseThrow();
    }

    private static Map<String, JsonValue> obj(JsonValue v) {
        return v.asObject().orElseThrow();
    }

    private static String str(JsonValue v) {
        return v.asString().orElseThrow();
    }

    private static Number num(JsonValue v) {
        return v.asNumber().orElseThrow();
    }

    private static JsonValue field(JsonValue row, String key) {
        return obj(row).get(key);
    }

    private static JsonValue query(String sql) {
        return SQL4Json.queryAsJsonValue(sql, DATA);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Simple CASE
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void simpleCaseStringMatching() {
        var result = arr(query(
                "SELECT name, CASE dept WHEN 'eng' THEN 'Engineering' WHEN 'hr' THEN 'Human Resources' ELSE 'Other' END AS dept_label FROM $r"));
        assertEquals(5, result.size());

        // Alice → eng → Engineering
        assertEquals("Engineering", str(field(result.get(0), "dept_label")));
        // Bob → sales → Other
        assertEquals("Other", str(field(result.get(1), "dept_label")));
        // Carol → eng → Engineering
        assertEquals("Engineering", str(field(result.get(2), "dept_label")));
        // Dave → hr → Human Resources
        assertEquals("Human Resources", str(field(result.get(3), "dept_label")));
        // Eve → eng → Engineering
        assertEquals("Engineering", str(field(result.get(4), "dept_label")));
    }

    @Test
    void simpleCaseNumberMatching() {
        var result = arr(query(
                "SELECT id, CASE id WHEN 1 THEN 'first' WHEN 2 THEN 'second' ELSE 'other' END AS pos FROM $r"));
        assertEquals(5, result.size());

        assertEquals("first", str(field(result.get(0), "pos")));
        assertEquals("second", str(field(result.get(1), "pos")));
        assertEquals("other", str(field(result.get(2), "pos")));
        assertEquals("other", str(field(result.get(3), "pos")));
        assertEquals("other", str(field(result.get(4), "pos")));
    }

    @Test
    void simpleCaseNoElseReturnsNull() {
        var result = arr(query(
                "SELECT name, CASE dept WHEN 'nonexistent' THEN 'found' END AS x FROM $r"));
        assertEquals(5, result.size());
        for (JsonValue row : result) {
            assertTrue(field(row, "x").isNull(),
                    "expected null when no ELSE and no branch matches");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Searched CASE
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void searchedCaseComparison() {
        var result = arr(query(
                "SELECT name, salary, CASE WHEN salary > 80000 THEN 'high' WHEN salary > 55000 THEN 'mid' ELSE 'low' END AS band FROM $r"));
        assertEquals(5, result.size());

        // Alice 90000 → high
        assertEquals("high", str(field(result.get(0), "band")));
        // Bob 50000 → low
        assertEquals("low", str(field(result.get(1), "band")));
        // Carol 110000 → high
        assertEquals("high", str(field(result.get(2), "band")));
        // Dave 60000 → mid
        assertEquals("mid", str(field(result.get(3), "band")));
        // Eve 75000 → mid
        assertEquals("mid", str(field(result.get(4), "band")));
    }

    @Test
    void searchedCaseAndOr() {
        var result = arr(query(
                "SELECT name, CASE WHEN dept = 'eng' AND salary > 100000 THEN 'senior eng' WHEN dept = 'eng' THEN 'eng' ELSE 'other' END AS role FROM $r"));
        assertEquals(5, result.size());

        // Alice: eng, 90000 → eng (not > 100000)
        assertEquals("eng", str(field(result.get(0), "role")));
        // Bob: sales → other
        assertEquals("other", str(field(result.get(1), "role")));
        // Carol: eng, 110000 → senior eng
        assertEquals("senior eng", str(field(result.get(2), "role")));
        // Dave: hr → other
        assertEquals("other", str(field(result.get(3), "role")));
        // Eve: eng, 75000 → eng
        assertEquals("eng", str(field(result.get(4), "role")));
    }

    @Test
    void searchedCaseLike() {
        var result = arr(query(
                "SELECT name, CASE WHEN name LIKE 'A%' THEN 'starts_a' ELSE 'other' END AS initial FROM $r"));
        assertEquals(5, result.size());

        // Alice → starts_a
        assertEquals("starts_a", str(field(result.get(0), "initial")));
        // Bob → other
        assertEquals("other", str(field(result.get(1), "initial")));
        // Carol → other
        assertEquals("other", str(field(result.get(2), "initial")));
    }

    @Test
    void searchedCaseIsNull() {
        var result = arr(query(
                "SELECT name, CASE WHEN status IS NULL THEN 'unknown' ELSE status END AS status_label FROM $r"));
        assertEquals(5, result.size());

        // Dave has status = null → unknown
        assertEquals("unknown", str(field(result.get(3), "status_label")));
        // Alice has status = 'active' → active
        assertEquals("active", str(field(result.get(0), "status_label")));
        // Bob has status = 'inactive' → inactive
        assertEquals("inactive", str(field(result.get(1), "status_label")));
    }

    // ═══════════════════════════════════════════════════════════════════
    // Usability in clauses
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void caseInSelectWithAlias() {
        var result = arr(query(
                "SELECT CASE WHEN salary > 80000 THEN 'high' ELSE 'low' END AS level FROM $r"));
        assertEquals(5, result.size());

        // All rows should have a "level" field
        for (JsonValue row : result) {
            var level = field(row, "level");
            assertFalse(level.isNull(), "level should not be null");
            assertTrue(level.isString(), "level should be a string");
            var val = str(level);
            assertTrue(val.equals("high") || val.equals("low"),
                    "level should be 'high' or 'low', got: " + val);
        }
    }

    @Test
    void caseInWhere() {
        var result = arr(query(
                "SELECT name FROM $r WHERE CASE WHEN salary > 80000 THEN 'high' ELSE 'low' END = 'high'"));
        // Alice (90000) and Carol (110000) → 2 rows
        assertEquals(2, result.size());

        var names = result.stream()
                .map(r -> str(field(r, "name")))
                .toList();
        assertTrue(names.contains("Alice"));
        assertTrue(names.contains("Carol"));
    }

    @Test
    void caseInOrderBy() {
        var result = arr(query(
                "SELECT name, dept FROM $r ORDER BY CASE dept WHEN 'eng' THEN 1 WHEN 'hr' THEN 2 ELSE 3 END ASC, name ASC"));
        assertEquals(5, result.size());

        // eng rows come first (order 1): Alice, Carol, Eve
        assertEquals("Alice", str(field(result.get(0), "name")));
        assertEquals("Carol", str(field(result.get(1), "name")));
        assertEquals("Eve", str(field(result.get(2), "name")));
        // hr row comes next (order 2): Dave
        assertEquals("Dave", str(field(result.get(3), "name")));
        // sales row comes last (order 3): Bob
        assertEquals("Bob", str(field(result.get(4), "name")));
    }

    @Test
    void caseInGroupBy() {
        var result = arr(query(
                "SELECT CASE WHEN salary > 80000 THEN 'high' ELSE 'low' END AS level, COUNT(*) AS cnt FROM $r GROUP BY CASE WHEN salary > 80000 THEN 'high' ELSE 'low' END ORDER BY level ASC"));
        assertEquals(2, result.size());

        // high: Alice (90000), Carol (110000) → 2
        // low: Bob (50000), Dave (60000), Eve (75000) → 3
        var highRow = result.stream()
                .filter(r -> "high".equals(str(field(r, "level"))))
                .findFirst().orElseThrow();
        var lowRow = result.stream()
                .filter(r -> "low".equals(str(field(r, "level"))))
                .findFirst().orElseThrow();

        assertEquals(2, num(field(highRow, "cnt")).intValue());
        assertEquals(3, num(field(lowRow, "cnt")).intValue());
    }

    @Test
    void caseInHaving() {
        var result = arr(query(
                "SELECT CASE WHEN salary > 80000 THEN 'high' ELSE 'low' END AS level, COUNT(*) AS cnt FROM $r GROUP BY CASE WHEN salary > 80000 THEN 'high' ELSE 'low' END HAVING cnt > 2"));
        // Only the 'low' group has cnt=3 which is > 2; 'high' has cnt=2
        assertEquals(1, result.size());
        assertEquals("low", str(field(result.get(0), "level")));
        assertEquals(3, num(field(result.get(0), "cnt")).intValue());
    }

    // ═══════════════════════════════════════════════════════════════════
    // Nesting
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void caseInsideFunction() {
        var result = arr(query(
                "SELECT name, UPPER(CASE WHEN status = 'active' THEN 'yes' ELSE 'no' END) AS is_active FROM $r"));
        assertEquals(5, result.size());

        // Active employees → YES
        assertEquals("YES", str(field(result.get(0), "is_active"))); // Alice
        assertEquals("NO", str(field(result.get(1), "is_active")));  // Bob (inactive)
        assertEquals("YES", str(field(result.get(2), "is_active"))); // Carol
        // Dave has null status → ELSE 'no' → NO
        assertEquals("NO", str(field(result.get(3), "is_active")));
        assertEquals("YES", str(field(result.get(4), "is_active"))); // Eve
    }

    @Test
    void functionInsideCase() {
        var result = arr(query(
                "SELECT name, CASE WHEN LENGTH(name) > 3 THEN 'long' ELSE 'short' END AS name_len FROM $r"));
        assertEquals(5, result.size());

        // Alice(5) → long, Bob(3) → short, Carol(5) → long, Dave(4) → long, Eve(3) → short
        assertEquals("long", str(field(result.get(0), "name_len")));  // Alice
        assertEquals("short", str(field(result.get(1), "name_len"))); // Bob
        assertEquals("long", str(field(result.get(2), "name_len")));  // Carol
        assertEquals("long", str(field(result.get(3), "name_len")));  // Dave
        assertEquals("short", str(field(result.get(4), "name_len"))); // Eve
    }

    @Test
    void caseInsideCase() {
        var result = arr(query(
                "SELECT name, CASE WHEN dept = 'eng' THEN CASE WHEN salary > 100000 THEN 'senior' ELSE 'junior' END ELSE 'non-eng' END AS eng_level FROM $r"));
        assertEquals(5, result.size());

        assertEquals("junior", str(field(result.get(0), "eng_level")));  // Alice eng 90000
        assertEquals("non-eng", str(field(result.get(1), "eng_level"))); // Bob sales
        assertEquals("senior", str(field(result.get(2), "eng_level")));  // Carol eng 110000
        assertEquals("non-eng", str(field(result.get(3), "eng_level"))); // Dave hr
        assertEquals("junior", str(field(result.get(4), "eng_level")));  // Eve eng 75000
    }

    // ═══════════════════════════════════════════════════════════════════
    // Aggregates
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void caseWithAggregateInThen() {
        // CASE dept WHEN 'eng' THEN SUM(salary) ELSE 0 END with GROUP BY dept
        var result = arr(query(
                "SELECT dept, CASE dept WHEN 'eng' THEN SUM(salary) ELSE 0 END AS eng_total FROM $r GROUP BY dept ORDER BY dept ASC"));
        assertEquals(3, result.size()); // eng, hr, sales

        // eng: SUM = 90000 + 110000 + 75000 = 275000
        var engRow = result.stream()
                .filter(r -> "eng".equals(str(field(r, "dept"))))
                .findFirst().orElseThrow();
        assertEquals(275000, num(field(engRow, "eng_total")).intValue());

        // hr: ELSE → 0
        var hrRow = result.stream()
                .filter(r -> "hr".equals(str(field(r, "dept"))))
                .findFirst().orElseThrow();
        assertEquals(0, num(field(hrRow, "eng_total")).intValue());

        // sales: ELSE → 0
        var salesRow = result.stream()
                .filter(r -> "sales".equals(str(field(r, "dept"))))
                .findFirst().orElseThrow();
        assertEquals(0, num(field(salesRow, "eng_total")).intValue());
    }

    @Test
    void simpleCaseWithCountStar() {
        // Group all rows by dept and use CASE on COUNT(*) per group.
        // eng dept has 3 rows → COUNT(*) = 3 → matches WHEN 3 → 'three'
        var result = arr(query(
                "SELECT dept, CASE COUNT(*) WHEN 3 THEN 'three' WHEN 1 THEN 'one' ELSE 'other' END AS answer FROM $r GROUP BY dept ORDER BY dept ASC"));
        // eng: 3 rows → three, hr: 1 row → one, sales: 1 row → one
        assertEquals(3, result.size());

        var engRow = result.stream()
                .filter(r -> "eng".equals(str(field(r, "dept"))))
                .findFirst().orElseThrow();
        assertEquals("three", str(field(engRow, "answer")));

        var hrRow = result.stream()
                .filter(r -> "hr".equals(str(field(r, "dept"))))
                .findFirst().orElseThrow();
        assertEquals("one", str(field(hrRow, "answer")));
    }

    // ═══════════════════════════════════════════════════════════════════
    // NULL semantics
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void simpleCaseNullNeverMatches() {
        // Simple CASE: WHEN NULL THEN 'matched' — should never match even for null status rows
        var result = arr(query(
                "SELECT name, CASE status WHEN NULL THEN 'matched' ELSE 'no match' END AS result FROM $r"));
        assertEquals(5, result.size());

        // All rows should be 'no match' — NULL never equals NULL in simple CASE
        for (JsonValue row : result) {
            assertEquals("no match", str(field(row, "result")),
                    "Simple CASE WHEN NULL should never match (SQL NULL semantics)");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Error cases
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void searchedCaseAggregateInConditionThrows() {
        // SUM in a CASE WHEN condition (not in THEN) is not valid — should throw parse or execution error
        assertThrows(Exception.class, () ->
                        SQL4Json.query("SELECT CASE WHEN SUM(salary) > 0 THEN 'pos' END FROM $r", DATA),
                "Aggregate function in CASE WHEN condition should throw an exception");
    }

    // ═══════════════════════════════════════════════════════════════════
    // NowRef
    // ═══════════════════════════════════════════════════════════════════

    @Test
    void nowInSelectReturnsFreshTimestamp() {
        var result = arr(query("SELECT NOW() AS ts FROM $r LIMIT 1"));
        assertEquals(1, result.size());

        var tsField = field(result.get(0), "ts");
        assertFalse(tsField.isNull(), "NOW() should not return null");
        // NOW() should serialize to a non-empty string
        String ts = str(tsField);
        assertFalse(ts.isBlank(), "NOW() timestamp should not be blank");
        // Should look like a date/time value (contains digits)
        assertTrue(ts.chars().anyMatch(Character::isDigit),
                "NOW() result should contain digits: " + ts);
    }

    @Test
    void dateDiffNowWithDateColumn() {
        var result = arr(query(
                "SELECT DATE_DIFF(NOW(), TO_DATE(hire_date), 'DAY') AS days FROM $r LIMIT 1"));
        assertEquals(1, result.size());

        var daysField = field(result.get(0), "days");
        assertFalse(daysField.isNull(), "DATE_DIFF(NOW(), hire_date) should not be null");
        // hire_date is 2020-03-15 which is well in the past → positive number of days
        assertTrue(num(daysField).longValue() > 0,
                "Days since hire_date should be positive, got: " + num(daysField));
    }
}
