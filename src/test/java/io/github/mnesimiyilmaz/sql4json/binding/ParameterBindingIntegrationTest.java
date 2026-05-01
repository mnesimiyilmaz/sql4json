package io.github.mnesimiyilmaz.sql4json.binding;

import io.github.mnesimiyilmaz.sql4json.BoundParameters;
import io.github.mnesimiyilmaz.sql4json.PreparedQuery;
import io.github.mnesimiyilmaz.sql4json.SQL4Json;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParameterBindingIntegrationTest {

    private static final String JSON = """
            [{"name":"Alice","age":30,"dept":"Eng"},
             {"name":"Bob","age":40,"dept":"HR"},
             {"name":"Carol","age":25,"dept":"Eng"}]
            """;

    @Test
    void when_named_parameters_bound_then_results_filtered() {
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name FROM $r WHERE age > :minAge AND dept = :dept");
        String r = q.execute(JSON, BoundParameters.named()
                .bind("minAge", 26).bind("dept", "Eng"));
        assertTrue(r.contains("Alice"));
        assertFalse(r.contains("Bob"));
        assertFalse(r.contains("Carol"));
    }

    @Test
    void when_positional_parameters_via_varargs_then_works() {
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name FROM $r WHERE age > ? AND dept = ?");
        String r = q.execute(JSON, 26, "Eng");
        assertTrue(r.contains("Alice"));
        assertFalse(r.contains("Bob"));
    }

    @Test
    void when_positional_parameters_via_BoundParameters_then_works() {
        PreparedQuery q = SQL4Json.prepare("SELECT name FROM $r WHERE age = ?");
        String r = q.execute(JSON, BoundParameters.of(40));
        assertTrue(r.contains("Bob"));
    }

    @Test
    void when_named_parameters_via_map_shortcut_then_works() {
        PreparedQuery q = SQL4Json.prepare("SELECT name FROM $r WHERE age > :min");
        String r = q.execute(JSON, java.util.Map.of("min", 28));
        assertTrue(r.contains("Alice"));
        assertTrue(r.contains("Bob"));
        assertFalse(r.contains("Carol"));
    }

    @Test
    void when_same_named_used_multiple_times_then_single_bind_is_enough() {
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name FROM $r WHERE dept = :d OR dept = :d");
        String r = q.execute(JSON, BoundParameters.named().bind("d", "HR"));
        assertTrue(r.contains("Bob"));
        assertFalse(r.contains("Alice"));
    }

    @Test
    void when_execute_called_with_bound_parameters_against_JsonValue_works() {
        var codec = io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings.defaults().codec();
        var data = codec.parse(JSON);
        PreparedQuery q = SQL4Json.prepare("SELECT name FROM $r WHERE dept = :d");
        var result = q.execute(data, BoundParameters.named().bind("d", "Eng"));
        assertNotNull(result);
    }

    @Test
    void when_parameterless_execute_unchanged() {
        // Regression guard: existing execute(String) path must still work with no params.
        PreparedQuery q = SQL4Json.prepare("SELECT name FROM $r WHERE age > 26");
        String r = q.execute(JSON);
        assertTrue(r.contains("Alice"));
        assertTrue(r.contains("Bob"));
        assertFalse(r.contains("Carol"));
    }

    /**
     * Simple record for typed-result binding tests.
     */
    record Person(String name, int age) {
    }

    @Test
    void when_executeAsList_with_parameters_then_mapped() {
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name, age FROM $r WHERE age > :min");
        java.util.List<Person> out = q.executeAsList(JSON, Person.class,
                BoundParameters.named().bind("min", 26));
        assertEquals(
                java.util.List.of(new Person("Alice", 30), new Person("Bob", 40)),
                out);
    }

    @Test
    void when_executeAs_single_row_with_parameters_then_mapped() {
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name, age FROM $r WHERE age = :age");
        Person p = q.executeAs(JSON, Person.class,
                BoundParameters.named().bind("age", 30));
        assertEquals(new Person("Alice", 30), p);
    }

    @Test
    void when_executeAsList_with_parameters_against_jsonvalue_then_mapped() {
        var codec = io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings.defaults().codec();
        var data = codec.parse(JSON);
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name, age FROM $r WHERE dept = :d");
        java.util.List<Person> out = q.executeAsList(data, Person.class,
                BoundParameters.named().bind("d", "Eng"));
        assertEquals(
                java.util.List.of(new Person("Alice", 30), new Person("Carol", 25)),
                out);
    }

    @Test
    void when_executeAs_with_parameters_against_jsonvalue_then_mapped() {
        var codec = io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings.defaults().codec();
        var data = codec.parse(JSON);
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name, age FROM $r WHERE age = :age");
        Person p = q.executeAs(data, Person.class,
                BoundParameters.named().bind("age", 40));
        assertEquals(new Person("Bob", 40), p);
    }

    @Test
    void when_engine_query_with_parameters_then_works() {
        io.github.mnesimiyilmaz.sql4json.SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        String r = engine.query("SELECT name FROM $r WHERE dept = :d",
                BoundParameters.named().bind("d", "HR"));
        assertTrue(r.contains("Bob"));
        assertFalse(r.contains("Alice"));
    }

    @Test
    void when_engine_cache_enabled_and_parameterized_query_then_cache_bypassed() {
        io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings s =
                io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings.builder()
                        .cache(c -> c.queryResultCacheEnabled(true)).build();
        io.github.mnesimiyilmaz.sql4json.SQL4JsonEngine engine = SQL4Json.engine()
                .settings(s).data(JSON).build();
        engine.query("SELECT name FROM $r WHERE dept = :d",
                BoundParameters.named().bind("d", "Eng"));
        engine.query("SELECT name FROM $r WHERE dept = :d",
                BoundParameters.named().bind("d", "HR"));
        assertEquals(0, engine.cacheSize(),
                "Parameterised queries must never populate the result cache");
    }

    @Test
    void when_engine_queryAsList_with_parameters_then_mapped() {
        io.github.mnesimiyilmaz.sql4json.SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        java.util.List<Person> out = engine.queryAsList(
                "SELECT name, age FROM $r WHERE age > :min",
                Person.class, BoundParameters.named().bind("min", 26));
        assertEquals(
                java.util.List.of(new Person("Alice", 30), new Person("Bob", 40)),
                out);
    }

    @Test
    void when_engine_queryAs_single_row_with_parameters_then_mapped() {
        io.github.mnesimiyilmaz.sql4json.SQL4JsonEngine engine = SQL4Json.engine().data(JSON).build();
        Person p = engine.queryAs(
                "SELECT name, age FROM $r WHERE age = :age",
                Person.class, BoundParameters.named().bind("age", 40));
        assertEquals(new Person("Bob", 40), p);
    }

    @Test
    void when_engine_query_without_parameters_cache_still_populates() {
        // Regression: non-parameterised queries on a cache-enabled engine still cache.
        io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings s =
                io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings.builder()
                        .cache(c -> c.queryResultCacheEnabled(true)).build();
        io.github.mnesimiyilmaz.sql4json.SQL4JsonEngine engine = SQL4Json.engine()
                .settings(s).data(JSON).build();
        engine.query("SELECT name FROM $r WHERE dept = 'Eng'");
        engine.query("SELECT name FROM $r WHERE dept = 'HR'");
        assertEquals(2, engine.cacheSize());
    }

    @Test
    void when_searched_case_in_select_uses_named_param_then_substituted() {
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name, CASE WHEN age > :threshold THEN 'adult' ELSE 'minor' END AS tier FROM $r");
        String r = q.execute(JSON, BoundParameters.named().bind("threshold", 28));
        assertTrue(r.contains("\"tier\":\"adult\""));
        assertTrue(r.contains("\"tier\":\"minor\""));
    }

    @Test
    void when_searched_case_in_select_uses_positional_param_then_substituted() {
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name, CASE WHEN age > ? THEN 'adult' ELSE 'minor' END AS tier FROM $r");
        String r = q.execute(JSON, BoundParameters.of(28));
        assertTrue(r.contains("\"tier\":\"adult\""));
        assertTrue(r.contains("\"tier\":\"minor\""));
    }

    @Test
    void when_searched_case_condition_uses_AND_with_params_then_all_substituted() {
        // Exercises the AndNode/OrNode recursion in substituteCriteria via a CASE WHEN.
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name, CASE WHEN age > :min AND dept = :d THEN 'match' ELSE 'other' END AS tag FROM $r");
        String r = q.execute(JSON, BoundParameters.named().bind("min", 26).bind("d", "Eng"));
        assertTrue(r.contains("\"name\":\"Alice\""));
        assertTrue(r.contains("\"tag\":\"match\""));
        assertTrue(r.contains("\"tag\":\"other\""));
    }

    @Test
    void when_simple_case_uses_named_param_in_when_value_then_substituted() {
        // Regression: simple CASE (param as a WHEN value, not inside a condition) — was already working.
        PreparedQuery q = SQL4Json.prepare(
                "SELECT name, CASE dept WHEN :d THEN 'primary' ELSE 'other' END AS bucket FROM $r");
        String r = q.execute(JSON, BoundParameters.named().bind("d", "Eng"));
        assertTrue(r.contains("\"bucket\":\"primary\""));
        assertTrue(r.contains("\"bucket\":\"other\""));
    }

    @Test
    void array_element_slot_rejects_collection_bind() {
        var pq = SQL4Json.prepare("SELECT * FROM $r WHERE tags @> ARRAY[?]");
        SQL4JsonBindException ex = assertThrows(SQL4JsonBindException.class, () ->
                pq.execute("[]", BoundParameters.of(java.util.List.of(java.util.List.of("a", "b")))));
        assertTrue(ex.getMessage().contains("ARRAY[?]"),
                "expected message to mention ARRAY[?], was: " + ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("scalar"),
                "expected message to mention 'scalar', was: " + ex.getMessage());
    }

    @Test
    void bare_array_rhs_rejects_scalar_bind() {
        var pq = SQL4Json.prepare("SELECT * FROM $r WHERE tags @> :req");
        SQL4JsonBindException ex = assertThrows(SQL4JsonBindException.class, () ->
                pq.execute("[]", BoundParameters.named().bind("req", "admin")));
        assertTrue(ex.getMessage().contains(":req"), ex.getMessage());
        assertTrue(ex.getMessage().contains("Collection"), ex.getMessage());
    }

    @Test
    void bare_array_rhs_rejects_map_bind() {
        var pq = SQL4Json.prepare("SELECT * FROM $r WHERE tags @> :req");
        SQL4JsonBindException ex = assertThrows(SQL4JsonBindException.class, () ->
                pq.execute("[]", BoundParameters.named().bind("req", java.util.Map.of("k", "v"))));
        assertTrue(ex.getMessage().contains("Collection"), ex.getMessage());
    }

    @Test
    void contains_keyword_rejects_collection_bind() {
        var pq = SQL4Json.prepare("SELECT * FROM $r WHERE tags CONTAINS :v");
        SQL4JsonBindException ex = assertThrows(SQL4JsonBindException.class, () ->
                pq.execute("[]", BoundParameters.named().bind("v", java.util.List.of("a"))));
        assertTrue(ex.getMessage().contains("CONTAINS"),
                "expected message to mention CONTAINS, was: " + ex.getMessage());
        assertTrue(ex.getMessage().toLowerCase().contains("scalar"),
                "expected message to mention 'scalar', was: " + ex.getMessage());
    }

    @Test
    void array_operator_accepts_java_array_bind() {
        String json = """
                [{"id": 1, "tags": ["admin","editor"]}, {"id": 2, "tags": ["viewer"]}]
                """;
        String result = SQL4Json.prepare("SELECT id FROM $r WHERE tags @> :req")
                .execute(json,
                        BoundParameters.named().bind("req", new String[]{"admin", "editor"}));
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }

    @Test
    void array_operator_rejects_null_bind() {
        var pq = SQL4Json.prepare("SELECT * FROM $r WHERE tags @> :req");
        SQL4JsonBindException ex = assertThrows(SQL4JsonBindException.class, () ->
                pq.execute("[]", BoundParameters.named().bind("req", (Object) null)));
        assertTrue(ex.getMessage().toLowerCase().contains("null"),
                "expected message to mention null, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("Collection"), ex.getMessage());
    }

    @Test
    void array_operator_with_column_ref_rhs_via_parameter_substitution() {
        // exercises substituteArrayPredicate Case C — column-ref via the substitution path
        String json = """
                [
                  {"id": 1, "tags": ["a","b"], "required": ["a"]},
                  {"id": 2, "tags": ["a"],     "required": ["b"]}
                ]
                """;
        // The :ignored parameter is unused but forces the substituteConditionContext path
        String result = SQL4Json.prepare(
                        "SELECT id FROM $r WHERE tags @> required AND id != :ignored")
                .execute(json, BoundParameters.named().bind("ignored", -1));
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }
}
