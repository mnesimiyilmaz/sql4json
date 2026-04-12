package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.json.JsonParser;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QueryExecutorTest {

    private QueryExecutor executor;

    @BeforeEach
    void setUp() {
        executor = new QueryExecutor();
    }

    private JsonValue execute(String sql, JsonValue data) {
        return executor.execute(QueryParser.parse(sql, Sql4jsonSettings.defaults()),
                data, Sql4jsonSettings.defaults());
    }

    // ── Short-circuit: SELECT * FROM $r ──────────────────────────────────────

    @Test
    void select_all_returns_all_rows() {
        JsonValue data = JsonParser.parse(
                "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]");
        JsonValue result = execute("SELECT * FROM $r", data);

        assertTrue(result.isArray());
        assertEquals(2, result.asArray().get().size());
    }

    // ── WHERE filter ─────────────────────────────────────────────────────────

    @Test
    void where_filters_rows() {
        JsonValue data = JsonParser.parse(
                "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20},{\"name\":\"Carol\",\"age\":35}]");
        JsonValue result = execute("SELECT * FROM $r WHERE age > 25", data);

        assertEquals(2, result.asArray().get().size()); // Alice + Carol
    }

    // ── SELECT specific columns ───────────────────────────────────────────────

    @Test
    void select_specific_column_excludes_others() {
        JsonValue data = JsonParser.parse(
                "[{\"name\":\"Alice\",\"age\":30,\"dept\":\"IT\"}]");
        JsonValue result = execute("SELECT name FROM $r", data);

        var row = result.asArray().get().getFirst().asObject().get();
        assertTrue(row.containsKey("name"));
        assertFalse(row.containsKey("age"));
        assertFalse(row.containsKey("dept"));
    }

    // ── ORDER BY ─────────────────────────────────────────────────────────────

    @Test
    void order_by_name_asc() {
        JsonValue data = JsonParser.parse(
                "[{\"name\":\"Charlie\"},{\"name\":\"Alice\"},{\"name\":\"Bob\"}]");
        JsonValue result = execute("SELECT * FROM $r ORDER BY name ASC", data);

        var rows = result.asArray().get();
        assertEquals("Alice", rows.get(0).asObject().get().get("name").asString().get());
        assertEquals("Bob", rows.get(1).asObject().get().get("name").asString().get());
        assertEquals("Charlie", rows.get(2).asObject().get().get("name").asString().get());
    }

    // ── GROUP BY + COUNT ──────────────────────────────────────────────────────

    @Test
    void group_by_count_returns_correct_group_count() {
        JsonValue data = JsonParser.parse(
                "[{\"dept\":\"IT\",\"name\":\"Alice\"}" +
                        ",{\"dept\":\"IT\",\"name\":\"Bob\"}" +
                        ",{\"dept\":\"HR\",\"name\":\"Carol\"}]");
        JsonValue result = execute(
                "SELECT dept, COUNT(name) AS cnt FROM $r GROUP BY dept", data);

        assertEquals(2, result.asArray().get().size());
    }

    // ── HAVING filter ─────────────────────────────────────────────────────────

    @Test
    void having_filters_groups_by_alias() {
        JsonValue data = JsonParser.parse(
                "[{\"dept\":\"IT\",\"name\":\"Alice\"}" +
                        ",{\"dept\":\"IT\",\"name\":\"Bob\"}" +
                        ",{\"dept\":\"HR\",\"name\":\"Carol\"}]");
        // Only groups with cnt > 1 survive
        JsonValue result = execute(
                "SELECT dept, COUNT(name) AS cnt FROM $r GROUP BY dept HAVING cnt > 1", data);

        var rows = result.asArray().get();
        assertEquals(1, rows.size());
        assertEquals("IT", rows.getFirst().asObject().get().get("dept").asString().get());
    }

    // ── FROM subquery ─────────────────────────────────────────────────────────

    @Test
    void from_subquery_chains_two_queries() {
        JsonValue data = JsonParser.parse(
                "[{\"name\":\"Alice\",\"age\":30}" +
                        ",{\"name\":\"Bob\",\"age\":20}" +
                        ",{\"name\":\"Carol\",\"age\":35}]");
        JsonValue result = execute(
                "SELECT name FROM (SELECT * FROM $r WHERE age > 25) WHERE name != 'Carol'",
                data);

        var rows = result.asArray().get();
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.getFirst().asObject().get().get("name").asString().get());
    }

    // ── Nested FROM path ─────────────────────────────────────────────────────

    @Test
    void nested_from_path_navigates_to_array() {
        JsonValue data = JsonParser.parse(
                "{\"data\":{\"items\":[{\"id\":1},{\"id\":2}]}}");
        JsonValue result = execute("SELECT * FROM $r.data.items", data);

        assertEquals(2, result.asArray().get().size());
    }

    // ── Alias in SELECT ───────────────────────────────────────────────────────

    @Test
    void select_with_alias_renames_output_field() {
        JsonValue data = JsonParser.parse("[{\"name\":\"Alice\"}]");
        JsonValue result = execute("SELECT name AS n FROM $r", data);

        var row = result.asArray().get().getFirst().asObject().get();
        assertTrue(row.containsKey("n"));
        assertFalse(row.containsKey("name"));
        assertEquals("Alice", row.get("n").asString().get());
    }

    // ── Streaming entry points ─────────────────────────────────────────

    @Test
    void executeStreaming_string_returns_same_as_tree_path() {
        String json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]";
        JsonCodec codec = new DefaultJsonCodec();
        Sql4jsonSettings settings = Sql4jsonSettings.defaults();

        JsonValue data = codec.parse(json);
        String treeResult = codec.serialize(execute("SELECT * FROM $r WHERE age > 25", data));
        String streamResult = executor.executeStreaming("SELECT * FROM $r WHERE age > 25", json, settings);

        assertEquals(treeResult, streamResult);
    }

    @Test
    void executeStreaming_with_query_definition() {
        String json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]";
        Sql4jsonSettings settings = Sql4jsonSettings.defaults();
        QueryDefinition query = QueryParser.parse("SELECT name FROM $r WHERE age > 25");

        String result = executor.executeStreaming(query, json, settings);
        assertTrue(result.contains("Alice"));
        assertFalse(result.contains("Bob"));
    }

    @Test
    void executeStreamingAsJsonValue_returns_json_array() {
        String json = "[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]";
        Sql4jsonSettings settings = Sql4jsonSettings.defaults();

        JsonValue result = executor.executeStreamingAsJsonValue(
                "SELECT * FROM $r", json, settings);
        assertTrue(result.isArray());
        assertEquals(2, result.asArray().orElseThrow().size());
    }

    @Test
    void executeStreaming_with_subquery() {
        String json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]";
        Sql4jsonSettings settings = Sql4jsonSettings.defaults();

        String result = executor.executeStreaming(
                "SELECT name FROM (SELECT * FROM $r WHERE age > 25)", json, settings);
        assertTrue(result.contains("Alice"));
        assertFalse(result.contains("Bob"));
    }

    @Test
    void executeStreaming_with_nested_path() {
        String json = "{\"data\":{\"items\":[{\"id\":1},{\"id\":2}]}}";
        Sql4jsonSettings settings = Sql4jsonSettings.defaults();

        String result = executor.executeStreaming(
                "SELECT * FROM $r.data.items", json, settings);
        assertTrue(result.contains("\"id\":1"));
        assertTrue(result.contains("\"id\":2"));
    }

    @Test
    void executeStreaming_select_all_no_clauses() {
        String json = "[{\"x\":1},{\"x\":2}]";
        Sql4jsonSettings settings = Sql4jsonSettings.defaults();

        String result = executor.executeStreaming("SELECT * FROM $r", json, settings);
        assertEquals("[{\"x\":1},{\"x\":2}]", result);
    }

    @Test
    void executeStreaming_with_limit() {
        String json = "[{\"id\":1},{\"id\":2},{\"id\":3},{\"id\":4},{\"id\":5}]";
        Sql4jsonSettings settings = Sql4jsonSettings.defaults();

        String result = executor.executeStreaming("SELECT * FROM $r LIMIT 2", json, settings);
        assertTrue(result.contains("\"id\":1"));
        assertTrue(result.contains("\"id\":2"));
        assertFalse(result.contains("\"id\":3"));
    }

    @Test
    void executeStreaming_with_group_by() {
        String json = "[{\"dept\":\"A\",\"sal\":10},{\"dept\":\"A\",\"sal\":20},{\"dept\":\"B\",\"sal\":30}]";
        JsonCodec codec = new DefaultJsonCodec();
        Sql4jsonSettings settings = Sql4jsonSettings.defaults();

        // Tree path
        JsonValue data = codec.parse(json);
        String treeResult = codec.serialize(
                execute("SELECT dept, SUM(sal) AS total FROM $r GROUP BY dept ORDER BY dept", data));

        // Streaming path
        String streamResult = executor.executeStreaming(
                "SELECT dept, SUM(sal) AS total FROM $r GROUP BY dept ORDER BY dept", json, settings);

        assertEquals(treeResult, streamResult);
    }
}
