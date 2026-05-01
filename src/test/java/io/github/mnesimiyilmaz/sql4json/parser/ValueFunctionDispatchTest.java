package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.BoundParameters;
import io.github.mnesimiyilmaz.sql4json.SQL4Json;
import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Confirms the C2 grammar refactor preserves NOW() semantics after VALUE_FUNCTION
 * is removed and NOW() parses through the generic functionCall rule.
 *
 * <p>Key invariant: NOW() that was originally lazy (per-row) in v1.1.x stays lazy
 * after the refactor. NOW() that was originally eager (CAST inner, nested function
 * args on RHS) stays eager.
 */
class ValueFunctionDispatchTest {

    @Test
    void nowInSelectEmitsNowRef() {
        var qd = QueryParser.parse("SELECT NOW() AS ts FROM $r");
        var expr = qd.selectedColumns().getFirst().expression();
        assertInstanceOf(Expression.NowRef.class, expr);
        assertTrue(qd.containsNonDeterministic());
    }

    @Test
    void nowInComparisonRhsStaysLazy() {
        // WHERE col > NOW() was NowRef (lazy per-row) in v1.1.x. The refactor must
        // preserve that — otherwise each row would be compared against a stale
        // parse-time snapshot of NOW().
        // Also verifies end-to-end execution: no ClassCastException or NPE.
        var qd = QueryParser.parse("SELECT * FROM $r WHERE age > NOW()");
        assertTrue(qd.containsNonDeterministic());

        // Runtime: age values are numeric, NOW() produces a date — no rows match,
        // but the query must complete without throwing.
        String json = "[{\"age\": 30}, {\"age\": 50}]";
        String result = SQL4Json.query("SELECT * FROM $r WHERE age > NOW()", json);
        assertNotNull(result);
    }

    @Test
    void nowInInListStaysLazy() {
        // IN-list element NOW() was also RhsPlainValue -> NowRef (lazy) in v1.1.x.
        // Pre-refactor this threw ClassCastException via InConditionHandler; must not throw.
        var qd = QueryParser.parse("SELECT * FROM $r WHERE age IN (NOW(), 1, 2)");
        assertTrue(qd.containsNonDeterministic());

        // Runtime: exercises InConditionHandler expression-eval path (NowRef in list).
        String json = "[{\"age\": 1}, {\"age\": 30}]";
        String result = SQL4Json.query("SELECT * FROM $r WHERE age IN (NOW(), 1, 2)", json);
        assertNotNull(result);
        assertTrue(result.contains("\"age\":1"));
    }

    @Test
    void nowInBetweenStaysLazy() {
        // BETWEEN 1 AND NOW() — NowRef on upper bound exercises BetweenConditionHandler
        // expression-eval path (upperBoundExpr != null).
        var qd = QueryParser.parse("SELECT * FROM $r WHERE age BETWEEN 1 AND NOW()");
        assertTrue(qd.containsNonDeterministic());

        // Runtime: no rows match (age 30/50 are not between 1 and a date string),
        // but the query must complete without throwing.
        String json = "[{\"age\": 30}, {\"age\": 50}]";
        String result = SQL4Json.query("SELECT * FROM $r WHERE age BETWEEN 1 AND NOW()", json);
        assertNotNull(result);
    }

    @Test
    void nowInsideCastIsEagerAndRuns() {
        // CAST(NOW() AS STRING) in WHERE RHS was eager (toSqlValue path) in v1.1.x.
        // It must still produce a usable SqlDateTime at parse time and run to
        // completion without throwing.
        String json = "[{\"n\": \"hello\"}]";
        String result = SQL4Json.query(
                "SELECT n FROM $r WHERE n != CAST(NOW() AS STRING)", json);
        assertTrue(result.contains("hello"));
    }

    @Test
    void nowInsideDateAddArgStaysNonDeterministic() {
        // DATE_ADD(NOW(), -1, 'DAY') — NowRef inside a nested function arg.
        // Also verifies end-to-end execution against a date-typed column.
        var qd = QueryParser.parse(
                "SELECT * FROM $r WHERE created > DATE_ADD(NOW(), -1, 'DAY')");
        assertTrue(qd.containsNonDeterministic());

        // Runtime: no rows expected (test values are not real dates), but must not throw.
        String json = "[{\"created\": \"2020-01-01T00:00:00\"}]";
        String result = SQL4Json.query(
                "SELECT * FROM $r WHERE created > DATE_ADD(NOW(), -1, 'DAY')", json);
        assertNotNull(result);
    }

    @Test
    void unknownZeroArgFunctionStillFails() {
        String json = "[{\"x\": 1}]";
        var ex = assertThrows(SQL4JsonExecutionException.class,
                () -> SQL4Json.query("SELECT bogus() AS b FROM $r", json));
        assertTrue(ex.getMessage().toLowerCase().contains("bogus"));
    }

    @Test
    void nowInParameterizedInListExecutes() {
        // Parameterized IN with NowRef — exercises ParameterSubstitutor.expandInListElement(NowRef).
        // NowRef is snapshotted once at substitution time for parameterized lists.
        String json = "[{\"age\": 30}, {\"age\": 50}]";
        var prepared = SQL4Json.prepare("SELECT * FROM $r WHERE age IN (NOW(), ?)");
        String result = prepared.execute(json, BoundParameters.of(List.of(30)));
        assertNotNull(result);
        assertTrue(result.contains("30"));
    }
}
