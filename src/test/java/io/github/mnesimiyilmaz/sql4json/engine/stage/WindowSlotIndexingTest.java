package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.SQL4Json;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WindowSlotIndexingTest {

    private static final String EMPLOYEES = """
            [
              {"name": "Alice", "dept": "ENG", "salary": 100},
              {"name": "Bob",   "dept": "ENG", "salary": 90},
              {"name": "Carol", "dept": "MKT", "salary": 80}
            ]""";

    @Test
    void plainAliasedWindow_resolvesByAlias() {
        String r = SQL4Json.query(
                "SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rk FROM $r",
                EMPLOYEES);
        assertTrue(r.contains("\"rk\":1"));
        assertTrue(r.contains("\"rk\":2"));
    }

    @Test
    void wrappedWindow_inRound_evaluatesViaExpressionTree() {
        String r = SQL4Json.query(
                "SELECT name, ROUND(AVG(salary) OVER (PARTITION BY dept), 2) AS avg_pay FROM $r",
                EMPLOYEES);
        assertTrue(r.contains("\"avg_pay\":95"));
        assertTrue(r.contains("\"avg_pay\":80"));
    }

    @Test
    void caseBuriedWindow_inWhenCondition() {
        String r = SQL4Json.query(
                "SELECT name, "
                + "CASE WHEN ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1 "
                + "     THEN 'top' ELSE 'other' END AS rank_label "
                + "FROM $r",
                EMPLOYEES);
        assertTrue(r.contains("\"rank_label\":\"top\""));
        assertTrue(r.contains("\"rank_label\":\"other\""));
    }

    @Test
    void caseBuriedWindow_inThenBranch() {
        String r = SQL4Json.query(
                "SELECT name, "
                + "CASE WHEN salary > 85 "
                + "     THEN ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) "
                + "     ELSE 0 END AS rk "
                + "FROM $r",
                EMPLOYEES);
        assertTrue(r.contains("\"rk\":1"));
        assertTrue(r.contains("\"rk\":0"));
    }

    @Test
    void orderByAlias_resolvesWindowedColumn() {
        String r = SQL4Json.query(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rk FROM $r ORDER BY rk",
                EMPLOYEES);
        int aliceIdx = r.indexOf("Alice");
        int carolIdx = r.indexOf("Carol");
        assertTrue(aliceIdx > 0 && carolIdx > 0);
        assertTrue(aliceIdx < carolIdx);
    }
}
