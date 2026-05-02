// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.settings;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.SQL4Json;
import io.github.mnesimiyilmaz.sql4json.engine.QueryExecutor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.json.JsonParser;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.Map;
import org.junit.jupiter.api.Test;

class Sql4jsonSettingsLimitsTests {

    @Test
    void queryParser_rejects_sql_above_maxSqlLength() {
        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxSqlLength(32)).build();
        String sql = "SELECT a, b, c FROM $r WHERE x > 1 AND y < 2";
        var ex = assertThrows(SQL4JsonParseException.class, () -> QueryParser.parse(sql, settings));
        assertTrue(ex.getMessage().contains("SQL query length exceeds configured maximum"));
    }

    @Test
    void inListSize_rejects_oversized_in_list() {
        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxInListSize(3)).build();
        String sql = "SELECT * FROM $r WHERE x IN (1, 2, 3, 4, 5)";
        var ex = assertThrows(SQL4JsonException.class, () -> QueryParser.parse(sql, settings));
        assertTrue(ex.getMessage().contains("IN list size exceeds configured maximum"));
    }

    @Test
    void notInListSize_rejects_oversized_not_in_list() {
        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxInListSize(3)).build();
        String sql = "SELECT * FROM $r WHERE x NOT IN (1, 2, 3, 4, 5)";
        var ex = assertThrows(SQL4JsonException.class, () -> QueryParser.parse(sql, settings));
        assertTrue(ex.getMessage().contains("IN list size exceeds configured maximum"));
    }

    @Test
    void maxRowsPerQuery_allows_group_by_at_exact_limit() {
        // 3 distinct keys, limit 3 → GROUP BY buffers exactly 3 input rows — must not throw
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < 3; i++) {
            if (i > 0) json.append(",");
            json.append("{\"k\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT k FROM $r GROUP BY k", settings);
        var data = JsonParser.parse(json.toString());

        assertDoesNotThrow(() -> executor.execute(query, data, settings));
    }

    @Test
    void maxRowsPerQuery_rejects_group_by_over_limit() {
        // 5 distinct keys, limit 3 → GROUP BY's materialization of input rows overflows
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < 5; i++) {
            if (i > 0) json.append(",");
            json.append("{\"k\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT k FROM $r GROUP BY k", settings);
        var data = JsonParser.parse(json.toString());

        var ex = assertThrows(SQL4JsonExecutionException.class, () -> executor.execute(query, data, settings));
        assertTrue(ex.getMessage().contains("GROUP BY row count exceeds configured maximum"));
    }

    @Test
    void maxRowsPerQuery_allows_order_by_at_exact_limit() {
        // 3 rows, limit 3 → ORDER BY buffers exactly 3 input rows — must not throw
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < 3; i++) {
            if (i > 0) json.append(",");
            json.append("{\"k\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT k FROM $r ORDER BY k", settings);
        var data = JsonParser.parse(json.toString());

        assertDoesNotThrow(() -> executor.execute(query, data, settings));
    }

    @Test
    void maxRowsPerQuery_rejects_order_by_over_limit() {
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < 5; i++) {
            if (i > 0) json.append(",");
            json.append("{\"k\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT k FROM $r ORDER BY k", settings);
        var data = JsonParser.parse(json.toString());

        var ex = assertThrows(SQL4JsonExecutionException.class, () -> executor.execute(query, data, settings));
        assertTrue(ex.getMessage().contains("ORDER BY row count exceeds configured maximum"));
    }

    @Test
    void maxRowsPerQuery_allows_window_at_exact_limit() {
        // 3 rows, limit 3 → WINDOW buffers exactly 3 input rows — must not throw
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < 3; i++) {
            if (i > 0) json.append(",");
            json.append("{\"k\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT k, ROW_NUMBER() OVER (ORDER BY k) AS rn FROM $r", settings);
        var data = JsonParser.parse(json.toString());

        assertDoesNotThrow(() -> executor.execute(query, data, settings));
    }

    @Test
    void maxRowsPerQuery_rejects_window_over_limit() {
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < 5; i++) {
            if (i > 0) json.append(",");
            json.append("{\"k\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT k, ROW_NUMBER() OVER (ORDER BY k) AS rn FROM $r", settings);
        var data = JsonParser.parse(json.toString());

        var ex = assertThrows(SQL4JsonExecutionException.class, () -> executor.execute(query, data, settings));
        assertTrue(ex.getMessage().contains("WINDOW row count exceeds configured maximum"));
    }

    @Test
    void maxRowsPerQuery_rejects_join_over_limit() {
        // 5 users, 5 matching orders → 5 joined rows, limit 3
        String usersJson = "[{\"id\":1},{\"id\":2},{\"id\":3},{\"id\":4},{\"id\":5}]";
        String ordersJson = "[{\"userId\":1,\"amt\":10},{\"userId\":2,\"amt\":20},"
                + "{\"userId\":3,\"amt\":30},{\"userId\":4,\"amt\":40},{\"userId\":5,\"amt\":50}]";

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT u.id, o.amt FROM users u JOIN orders o ON u.id = o.userId", settings);

        Map<String, JsonValue> sources = Map.of(
                "users", JsonParser.parse(usersJson),
                "orders", JsonParser.parse(ordersJson));

        var ex = assertThrows(SQL4JsonExecutionException.class, () -> executor.execute(query, sources, settings));
        assertTrue(ex.getMessage().contains("JOIN row count exceeds configured maximum"));
    }

    @Test
    void maxRowsPerQuery_allows_join_at_exact_limit() {
        String usersJson = "[{\"id\":1},{\"id\":2},{\"id\":3}]";
        String ordersJson = "[{\"userId\":1,\"amt\":10},{\"userId\":2,\"amt\":20},{\"userId\":3,\"amt\":30}]";

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT u.id, o.amt FROM users u JOIN orders o ON u.id = o.userId", settings);
        Map<String, JsonValue> sources = Map.of(
                "users", JsonParser.parse(usersJson),
                "orders", JsonParser.parse(ordersJson));

        assertDoesNotThrow(() -> executor.execute(query, sources, settings));
    }

    @Test
    void maxRowsPerQuery_allows_pipeline_at_exact_limit() {
        // 3 rows, limit 3 → PIPELINE terminal collects exactly 3 rows — must not throw
        StringBuilder json = new StringBuilder("[");
        for (int i = 1; i <= 3; i++) {
            if (i > 1) json.append(",");
            json.append("{\"x\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT x FROM $r WHERE x > 0", settings);
        var data = JsonParser.parse(json.toString());

        assertDoesNotThrow(() -> executor.execute(query, data, settings));
    }

    @Test
    void maxRowsPerQuery_rejects_pipeline_over_limit_on_select_where() {
        // 5 rows, limit 3 — no grouping/sorting/windowing, so only PIPELINE terminal fires
        StringBuilder json = new StringBuilder("[");
        for (int i = 1; i <= 5; i++) {
            if (i > 1) json.append(",");
            json.append("{\"x\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT x FROM $r WHERE x > 0", settings);
        var data = JsonParser.parse(json.toString());

        var ex = assertThrows(SQL4JsonExecutionException.class, () -> executor.execute(query, data, settings));
        assertTrue(ex.getMessage().contains("PIPELINE row count exceeds configured maximum"));
    }

    @Test
    void maxRowsPerQuery_allows_distinct_at_exact_limit() {
        // 3 distinct values, limit 3 → DISTINCT accumulates exactly 3 output rows — must not throw
        StringBuilder json = new StringBuilder("[");
        for (int i = 1; i <= 3; i++) {
            if (i > 1) json.append(",");
            json.append("{\"k\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT DISTINCT k FROM $r", settings);
        var data = JsonParser.parse(json.toString());

        assertDoesNotThrow(() -> executor.execute(query, data, settings));
    }

    @Test
    void maxRowsPerQuery_rejects_distinct_over_limit() {
        // 5 distinct values, limit 3 — DISTINCT materialization must overflow
        StringBuilder json = new StringBuilder("[");
        for (int i = 1; i <= 5; i++) {
            if (i > 1) json.append(",");
            json.append("{\"k\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT DISTINCT k FROM $r", settings);
        var data = JsonParser.parse(json.toString());

        var ex = assertThrows(SQL4JsonExecutionException.class, () -> executor.execute(query, data, settings));
        assertTrue(ex.getMessage().contains("DISTINCT row count exceeds configured maximum"));
    }

    @Test
    void maxRowsPerQuery_allows_streaming_at_exact_limit() {
        // 3 rows, limit 3 → STREAMING sink receives exactly 3 rows — must not throw
        StringBuilder json = new StringBuilder("[");
        for (int i = 1; i <= 3; i++) {
            if (i > 1) json.append(",");
            json.append("{\"x\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT x FROM $r WHERE x > 0", settings);

        assertDoesNotThrow(() -> executor.executeStreaming(query, json.toString(), settings));
    }

    @Test
    void maxRowsPerQuery_rejects_streaming_over_limit() {
        // 5 rows, limit 3 — streaming path must overflow via STREAMING guard
        StringBuilder json = new StringBuilder("[");
        for (int i = 1; i <= 5; i++) {
            if (i > 1) json.append(",");
            json.append("{\"x\":").append(i).append("}");
        }
        json.append("]");

        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxRowsPerQuery(3)).build();
        var executor = new QueryExecutor();
        var query = QueryParser.parse("SELECT x FROM $r WHERE x > 0", settings);

        var ex = assertThrows(
                SQL4JsonExecutionException.class, () -> executor.executeStreaming(query, json.toString(), settings));
        assertTrue(ex.getMessage().contains("STREAMING row count exceeds configured maximum"));
    }

    @Test
    void queryAsJsonValue_jsonvalue_overload_enforces_maxSqlLength() {
        // Regression: prove the JsonValue-data overload threads settings to the parser
        // and does not silently fall back to Sql4jsonSettings.defaults().
        var settings =
                Sql4jsonSettings.builder().limits(l -> l.maxSqlLength(32)).build();
        String longSql = "SELECT a, b, c FROM $r WHERE x > 1 AND y < 2"; // > 32 chars
        var data = JsonParser.parse("[{\"a\":1}]");
        var ex = assertThrows(SQL4JsonParseException.class, () -> SQL4Json.queryAsJsonValue(longSql, data, settings));
        assertTrue(ex.getMessage().contains("SQL query length exceeds configured maximum"));
    }
}
