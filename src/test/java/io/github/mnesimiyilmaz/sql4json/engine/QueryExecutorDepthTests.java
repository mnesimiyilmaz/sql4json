// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueryExecutorDepthTests {

    @Test
    void subqueryDepth_rejects_over_limit() {
        // Wrap SELECT * FROM $r 5 times → 5 FROM subqueries deep
        StringBuilder sql = new StringBuilder("SELECT * FROM $r");
        for (int i = 0; i < 5; i++) {
            sql = new StringBuilder("SELECT * FROM (").append(sql).append(")");
        }
        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxSubqueryDepth(3)).build();
        QueryDefinition qd = QueryParser.parse(sql.toString(), settings);
        QueryExecutor executor = new QueryExecutor();
        JsonValue data = new JsonArrayValue(List.of());

        var ex = assertThrows(SQL4JsonException.class, () -> executor.execute(qd, data, settings));
        assertTrue(
                ex.getMessage().contains("Subquery depth exceeds configured maximum"),
                "actual message: " + ex.getMessage());
    }

    @Test
    void subqueryDepth_accepts_at_exact_limit() {
        // 3 wraps = depth 3 — exactly at limit
        StringBuilder sql = new StringBuilder("SELECT * FROM $r");
        for (int i = 0; i < 3; i++) {
            sql = new StringBuilder("SELECT * FROM (").append(sql).append(")");
        }
        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxSubqueryDepth(3)).build();
        QueryDefinition qd = QueryParser.parse(sql.toString(), settings);
        QueryExecutor executor = new QueryExecutor();
        JsonValue data = new JsonArrayValue(List.of());

        assertDoesNotThrow(() -> executor.execute(qd, data, settings));
    }

    @Test
    void streamingSubqueryDepth_rejects_over_limit() {
        StringBuilder sql = new StringBuilder("SELECT * FROM $r");
        for (int i = 0; i < 5; i++) {
            sql = new StringBuilder("SELECT * FROM (").append(sql).append(")");
        }
        String finalSql = sql.toString();
        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxSubqueryDepth(3)).build();
        QueryExecutor executor = new QueryExecutor();
        String json = "[]";

        var ex = assertThrows(SQL4JsonException.class, () -> executor.executeStreaming(finalSql, json, settings));
        assertTrue(ex.getMessage().contains("Subquery depth exceeds configured maximum"), "actual: " + ex.getMessage());
    }

    @Test
    void streamingSubqueryDepth_accepts_at_exact_limit() {
        StringBuilder sql = new StringBuilder("SELECT * FROM $r");
        for (int i = 0; i < 3; i++) {
            sql = new StringBuilder("SELECT * FROM (").append(sql).append(")");
        }
        String finalSql = sql.toString();
        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxSubqueryDepth(3)).build();
        QueryExecutor executor = new QueryExecutor();
        String json = "[]";

        assertDoesNotThrow(() -> executor.executeStreaming(finalSql, json, settings));
    }
}
