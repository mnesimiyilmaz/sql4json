// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.engine.QueryExecutor;
import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.json.JsonParser;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JoinQueryTest {

    static final String USERS = """
            [
              {"id": 1, "name": "Alice", "dept": "Engineering"},
              {"id": 2, "name": "Bob", "dept": "Marketing"},
              {"id": 3, "name": "Charlie", "dept": "Engineering"}
            ]""";

    static final String ORDERS = """
            [
              {"order_id": 101, "user_id": 1, "amount": 250.00, "status": "completed"},
              {"order_id": 102, "user_id": 1, "amount": 150.00, "status": "pending"},
              {"order_id": 103, "user_id": 2, "amount": 350.00, "status": "completed"},
              {"order_id": 104, "user_id": 999, "amount": 50.00, "status": "completed"}
            ]""";

    // ── Static API ──────────────────────────────────────────────────

    @Test
    void static_api_inner_join() {
        String sql = """
                SELECT u.name AS name, o.amount AS amount
                FROM users u JOIN orders o ON u.id = o.user_id""";
        JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("users", USERS, "orders", ORDERS));

        var arr = result.asArray().orElseThrow();
        assertEquals(3, arr.size()); // Alice has 2 orders, Bob has 1
    }

    @Test
    void static_api_left_join() {
        String sql = """
                SELECT u.name AS name, o.amount AS amount
                FROM users u LEFT JOIN orders o ON u.id = o.user_id""";
        JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("users", USERS, "orders", ORDERS));

        var arr = result.asArray().orElseThrow();
        assertEquals(4, arr.size()); // Alice(2) + Bob(1) + Charlie(1 with null)

        // Charlie has no orders — amount should be null
        var charlie = arr.stream()
                .filter(v -> v.asObject()
                        .map(m -> m.get("name"))
                        .flatMap(JsonValue::asString)
                        .map("Charlie"::equals)
                        .orElse(false))
                .findFirst()
                .orElseThrow();
        assertTrue(charlie.asObject().orElseThrow().get("amount").isNull());
    }

    @Test
    void static_api_right_join() {
        String sql = """
                SELECT u.name AS name, o.amount AS amount
                FROM users u RIGHT JOIN orders o ON u.id = o.user_id""";
        JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("users", USERS, "orders", ORDERS));

        var arr = result.asArray().orElseThrow();
        assertEquals(4, arr.size()); // 3 matched + 1 unmatched order (user_id=999)

        // Unmatched order should have null name
        var unmatched = arr.stream()
                .filter(v -> v.asObject()
                        .map(m -> m.get("name"))
                        .map(JsonValue::isNull)
                        .orElse(false))
                .findFirst()
                .orElseThrow();
        assertEquals(
                50.0,
                unmatched
                        .asObject()
                        .orElseThrow()
                        .get("amount")
                        .asNumber()
                        .orElseThrow()
                        .doubleValue());
    }

    @Test
    void static_api_join_with_where() {
        String sql = """
                SELECT u.name AS name, o.amount AS amount
                FROM users u JOIN orders o ON u.id = o.user_id
                WHERE o.status = 'completed'""";
        JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("users", USERS, "orders", ORDERS));

        var arr = result.asArray().orElseThrow();
        assertEquals(2, arr.size()); // Alice(250) + Bob(350)
    }

    @Test
    void static_api_join_with_group_by() {
        String sql = """
                SELECT u.dept AS dept, COUNT(*) AS cnt, SUM(o.amount) AS total
                FROM users u JOIN orders o ON u.id = o.user_id
                WHERE o.status = 'completed'
                GROUP BY u.dept
                ORDER BY total DESC""";
        JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("users", USERS, "orders", ORDERS));

        var arr = result.asArray().orElseThrow();
        assertEquals(2, arr.size());

        // Marketing (Bob: 350) should be first (ORDER BY total DESC)
        var first = arr.getFirst().asObject().orElseThrow();
        assertEquals("Marketing", first.get("dept").asString().orElseThrow());
    }

    @Test
    void static_api_query_returns_string() {
        String sql = "SELECT u.name AS name FROM users u JOIN orders o ON u.id = o.user_id";
        String result = SQL4Json.query(sql, Map.of("users", USERS, "orders", ORDERS));
        assertNotNull(result);
        assertTrue(result.contains("Alice"));
    }

    // ── Output format ───────────────────────────────────────────────

    @Test
    void non_aliased_columns_produce_flat_dotted_keys() {
        String sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id";
        JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("users", USERS, "orders", ORDERS));

        var first = result.asArray().orElseThrow().getFirst().asObject().orElseThrow();
        // Non-aliased dotted columns are output as flat keys (e.g. "u.name")
        assertEquals("Alice", first.get("u.name").asString().orElseThrow());
        assertNotNull(first.get("o.amount"));
    }

    // ── Engine API ──────────────────────────────────────────────────

    @Test
    void engine_api_named_sources() {
        SQL4JsonEngine engine =
                SQL4Json.engine().data("users", USERS).data("orders", ORDERS).build();

        JsonValue result = engine.queryAsJsonValue(
                "SELECT u.name AS name, o.amount AS amount FROM users u JOIN orders o ON u.id = o.user_id");

        assertEquals(3, result.asArray().orElseThrow().size());
    }

    @Test
    void engine_api_named_sources_with_json_value() {
        JsonValue usersData = new DefaultJsonCodec().parse(USERS);
        JsonValue ordersData = new DefaultJsonCodec().parse(ORDERS);

        SQL4JsonEngine engine = SQL4Json.engine()
                .data("users", usersData)
                .data("orders", ordersData)
                .build();

        JsonValue result =
                engine.queryAsJsonValue("SELECT u.name AS name FROM users u JOIN orders o ON u.id = o.user_id");
        assertEquals(3, result.asArray().orElseThrow().size());
    }

    @Test
    void engine_api_mixed_named_and_unnamed() {
        SQL4JsonEngine engine = SQL4Json.engine()
                .data(USERS) // unnamed — for $r queries
                .data("users", USERS)
                .data("orders", ORDERS)
                .build();

        // $r query uses unnamed source
        JsonValue r1 = engine.queryAsJsonValue("SELECT name FROM $r");
        assertEquals(3, r1.asArray().orElseThrow().size());

        // JOIN query uses named sources
        JsonValue r2 = engine.queryAsJsonValue("SELECT u.name AS name FROM users u JOIN orders o ON u.id = o.user_id");
        assertEquals(3, r2.asArray().orElseThrow().size());
    }

    // ── Error cases ─────────────────────────────────────────────────

    @Test
    void unknown_table_throws() {
        // default settings have redactErrorDetails=false, so the message includes the missing key + available set
        var ex = assertThrows(
                Exception.class,
                () -> SQL4Json.queryAsJsonValue(
                        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id", Map.of("users", USERS)));
        assertTrue(ex.getMessage().contains("orders"), "message must include the missing table name");
        assertTrue(ex.getMessage().contains("users"), "message must include available keys");
    }

    @Test
    void engine_join_without_named_sources_throws() {
        SQL4JsonEngine engine = SQL4Json.engine().data(USERS).build();
        assertThrows(
                Exception.class,
                () -> engine.queryAsJsonValue("SELECT u.name AS name FROM users u JOIN orders o ON u.id = o.user_id"));
    }

    // ── Backward compatibility ──────────────────────────────────────

    @Test
    void existing_single_source_api_unchanged() {
        String result = SQL4Json.query(
                "SELECT name FROM $r WHERE age > 25",
                "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":20}]");
        assertTrue(result.contains("Alice"));
        assertFalse(result.contains("Bob"));
    }

    // ── Additional error cases ──────────────────────────────────────

    @Test
    void on_condition_non_equality_operator_throws_parse_exception() {
        assertThrows(
                io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException.class,
                () -> SQL4Json.queryAsJsonValue(
                        "SELECT * FROM a a1 JOIN b b1 ON a1.x > b1.x", Map.of("a", "[]", "b", "[]")));
    }

    @Test
    void unknown_table_in_from_throws() {
        var settings = Sql4jsonSettings.builder()
                .security(s -> s.redactErrorDetails(false))
                .build();
        var executor = new QueryExecutor();
        var qd = QueryParser.parse("SELECT * FROM missing m JOIN orders o ON m.id = o.user_id");
        Map<String, JsonValue> sources = Map.of("orders", JsonParser.parse("[]"));
        var ex = assertThrows(
                io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException.class,
                () -> executor.execute(qd, sources, settings));
        assertTrue(ex.getMessage().contains("missing"));
        assertTrue(ex.getMessage().contains("Available"));
    }

    @Test
    void unknown_table_in_join_throws() {
        var settings = Sql4jsonSettings.builder()
                .security(s -> s.redactErrorDetails(false))
                .build();
        var executor = new QueryExecutor();
        var qd = QueryParser.parse("SELECT * FROM users u JOIN missing m ON u.id = m.id");
        Map<String, JsonValue> sources = Map.of("users", JsonParser.parse("[]"));
        var ex = assertThrows(
                io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException.class,
                () -> executor.execute(qd, sources, settings));
        assertTrue(ex.getMessage().contains("missing"));
    }

    // ── Edge cases ──────────────────────────────────────────────────

    @Test
    void join_with_empty_left_table() {
        JsonValue result = SQL4Json.queryAsJsonValue(
                "SELECT u.name AS name, o.amount AS amount FROM users u JOIN orders o ON u.id = o.user_id",
                Map.of("users", "[]", "orders", ORDERS));
        assertTrue(result.asArray().orElseThrow().isEmpty());
    }

    @Test
    void join_with_empty_right_table() {
        JsonValue result = SQL4Json.queryAsJsonValue(
                "SELECT u.name AS name, o.amount AS amount FROM users u JOIN orders o ON u.id = o.user_id",
                Map.of("users", USERS, "orders", "[]"));
        assertTrue(result.asArray().orElseThrow().isEmpty());
    }

    @Test
    void left_join_with_empty_right_table() {
        JsonValue result = SQL4Json.queryAsJsonValue(
                "SELECT u.name AS name, o.amount AS amount FROM users u LEFT JOIN orders o ON u.id = o.user_id",
                Map.of("users", USERS, "orders", "[]"));
        var arr = result.asArray().orElseThrow();
        assertEquals(3, arr.size()); // all users, with null amounts
        arr.forEach(v -> assertTrue(v.asObject().orElseThrow().get("amount").isNull()));
    }

    @Test
    void join_with_null_key_values() {
        String left = """
                [{"id": null, "name": "Null-ID"}]""";
        String right = """
                [{"user_id": null, "val": "also-null"}]""";
        JsonValue result = SQL4Json.queryAsJsonValue(
                "SELECT l.name AS name, r.val AS val FROM left_t l JOIN right_t r ON l.id = r.user_id",
                Map.of("left_t", left, "right_t", right));
        assertEquals(1, result.asArray().orElseThrow().size());
    }

    @Test
    void join_with_select_asterisk() {
        JsonValue result = SQL4Json.queryAsJsonValue(
                "SELECT * FROM users u JOIN orders o ON u.id = o.user_id", Map.of("users", USERS, "orders", ORDERS));
        var arr = result.asArray().orElseThrow();
        assertEquals(3, arr.size());
        var first = arr.getFirst().asObject().orElseThrow();
        // Fields are nested under alias keys
        assertTrue(first.containsKey("u"));
        assertTrue(first.containsKey("o"));
    }

    @Test
    void join_with_limit_and_offset() {
        String sql = """
                SELECT u.name AS name, o.amount AS amount
                FROM users u JOIN orders o ON u.id = o.user_id
                ORDER BY o.amount DESC
                LIMIT 2 OFFSET 1""";
        JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("users", USERS, "orders", ORDERS));
        assertEquals(2, result.asArray().orElseThrow().size());
    }

    @Test
    void join_with_distinct() {
        String sql = "SELECT DISTINCT u.dept AS dept FROM users u JOIN orders o ON u.id = o.user_id";
        JsonValue result = SQL4Json.queryAsJsonValue(sql, Map.of("users", USERS, "orders", ORDERS));
        var arr = result.asArray().orElseThrow();
        // Join produces 3 rows (Alice×2 orders + Bob×1 order); each row retains all
        // merged fields so DISTINCT sees 3 unique combinations (orders 101 and 102 differ).
        assertEquals(3, arr.size());
    }

    @Test
    void chained_three_way_join() {
        String products = """
                [{"id": 1, "product_name": "Widget"},
                 {"id": 2, "product_name": "Gadget"}]""";
        String orderItems = """
                [{"order_id": 101, "product_id": 1},
                 {"order_id": 103, "product_id": 2}]""";

        String sql = """
                SELECT u.name AS user_name, o.order_id AS oid, p.product_name AS product
                FROM users u
                JOIN orders o ON u.id = o.user_id
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN products p ON oi.product_id = p.id""";

        JsonValue result = SQL4Json.queryAsJsonValue(
                sql, Map.of("users", USERS, "orders", ORDERS, "order_items", orderItems, "products", products));

        var arr = result.asArray().orElseThrow();
        assertEquals(2, arr.size()); // order 101 → Widget, order 103 → Gadget
    }

    // ── Nested field joins ──────────────────────────────────────────

    @Nested
    class NestedFieldTests {

        private static final String NESTED_USERS = """
                [
                  {"id": 1, "name": "Alice", "address": {"city": "Istanbul", "country": "TR", "geo": {"lat": 41.0}}, "profile": {"level": 5}},
                  {"id": 2, "name": "Bob", "address": {"city": "Berlin", "country": "DE", "geo": {"lat": 52.5}}, "profile": {"level": 3}},
                  {"id": 3, "name": "Carol", "address": null, "profile": {"level": 7}},
                  {"id": 4, "name": "Dave"}
                ]
                """;

        private static final String NESTED_ORDERS = """
                [
                  {"order_id": 101, "user_id": 1, "total": 250, "shipping": {"city": "Istanbul", "method": "express"}},
                  {"order_id": 102, "user_id": 2, "total": 180, "shipping": {"city": "Berlin", "method": "standard"}},
                  {"order_id": 103, "user_id": 1, "total": 75, "shipping": {"city": "Ankara", "method": "express"}},
                  {"order_id": 104, "user_id": 5, "total": 300, "shipping": {"city": "Paris", "method": "express"}}
                ]
                """;

        private static final Map<String, String> DATA = Map.of("users", NESTED_USERS, "orders", NESTED_ORDERS);

        @Test
        void innerJoinOnNestedField() {
            String sql = """
                    SELECT u.name AS name, u.address.city AS city
                    FROM users u JOIN orders o ON u.address.city = o.shipping.city""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // Alice/Istanbul (order 101) + Bob/Berlin (order 102) = 2 rows
            assertEquals(2, arr.size());
        }

        @Test
        void multiColumnOnFlatAndNested() {
            String sql = """
                    SELECT u.name AS name, o.order_id AS order_id
                    FROM users u JOIN orders o ON u.id = o.user_id AND u.address.city = o.shipping.city""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // Alice id=1 & city=Istanbul matches order 101; Bob id=2 & city=Berlin matches order 102
            assertEquals(2, arr.size());
        }

        @Test
        void selectNestedFieldsFromBothSides() {
            String sql = """
                    SELECT u.address.city AS user_city, o.shipping.method AS method
                    FROM users u JOIN orders o ON u.id = o.user_id""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // Alice has 2 orders, Bob has 1 → 3 rows
            assertEquals(3, arr.size());

            // All rows must have the user_city and method columns
            for (JsonValue row : arr) {
                Map<String, JsonValue> obj = row.asObject().orElseThrow();
                assertNotNull(obj.get("user_city"));
                assertNotNull(obj.get("method"));
            }
        }

        @Test
        void whereOnNestedJoinedField() {
            String sql = """
                    SELECT u.name AS name, o.order_id AS order_id
                    FROM users u JOIN orders o ON u.id = o.user_id
                    WHERE u.address.country = 'TR'""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // Alice (TR) has orders 101 and 103 → 2 rows
            assertEquals(2, arr.size());
            for (JsonValue row : arr) {
                assertEquals(
                        "Alice",
                        row.asObject().orElseThrow().get("name").asString().orElseThrow());
            }
        }

        @Test
        void groupByNestedJoinedField() {
            String sql = """
                    SELECT u.address.country AS country, SUM(o.total) AS total_sum
                    FROM users u JOIN orders o ON u.id = o.user_id
                    GROUP BY u.address.country""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // TR (Alice: 250+75=325) and DE (Bob: 180) → 2 groups
            assertEquals(2, arr.size());
        }

        @Test
        void orderByNestedJoinedField() {
            String sql = """
                    SELECT u.name AS name, o.shipping.city AS ship_city
                    FROM users u JOIN orders o ON u.id = o.user_id
                    ORDER BY o.shipping.city ASC""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            assertEquals(3, arr.size());
            // Alphabetical by shipping city: Ankara (order 103), Berlin (order 102), Istanbul (order 101)
            assertEquals(
                    "Ankara",
                    arr.get(0)
                            .asObject()
                            .orElseThrow()
                            .get("ship_city")
                            .asString()
                            .orElseThrow());
            assertEquals(
                    "Berlin",
                    arr.get(1)
                            .asObject()
                            .orElseThrow()
                            .get("ship_city")
                            .asString()
                            .orElseThrow());
            assertEquals(
                    "Istanbul",
                    arr.get(2)
                            .asObject()
                            .orElseThrow()
                            .get("ship_city")
                            .asString()
                            .orElseThrow());
        }

        @Test
        void leftJoinWithNullNestedObject() {
            String sql = """
                    SELECT u.name AS name, o.order_id AS order_id
                    FROM users u LEFT JOIN orders o ON u.id = o.user_id""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // Alice(2) + Bob(1) + Carol(1 null) + Dave(1 null) = 5 rows
            assertEquals(5, arr.size());

            // Carol appears with null order_id
            JsonValue carolRow = arr.stream()
                    .filter(v -> v.asObject()
                            .map(m -> m.get("name"))
                            .flatMap(JsonValue::asString)
                            .map("Carol"::equals)
                            .orElse(false))
                    .findFirst()
                    .orElseThrow();
            assertTrue(carolRow.asObject().orElseThrow().get("order_id").isNull());
        }

        @Test
        void leftJoinWithMissingNestedPath() {
            String sql = """
                    SELECT u.name AS name, u.address.city AS city, o.order_id AS order_id
                    FROM users u LEFT JOIN orders o ON u.id = o.user_id""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // Alice(2) + Bob(1) + Carol(1 null) + Dave(1 null) = 5 rows
            assertEquals(5, arr.size());

            // Dave appears with null city and null order_id
            JsonValue daveRow = arr.stream()
                    .filter(v -> v.asObject()
                            .map(m -> m.get("name"))
                            .flatMap(JsonValue::asString)
                            .map("Dave"::equals)
                            .orElse(false))
                    .findFirst()
                    .orElseThrow();
            Map<String, JsonValue> daveObj = daveRow.asObject().orElseThrow();
            assertTrue(daveObj.get("order_id").isNull());
            assertTrue(daveObj.get("city").isNull());
        }

        @Test
        void rightJoinWithNestedFields() {
            String sql = """
                    SELECT u.name AS name, o.order_id AS order_id, o.shipping.city AS ship_city
                    FROM users u RIGHT JOIN orders o ON u.id = o.user_id""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // Alice(2) + Bob(1) + order104(1 unmatched) = 4 rows
            assertEquals(4, arr.size());

            // Unmatched order 104 should have null name
            JsonValue unmatched = arr.stream()
                    .filter(v -> v.asObject()
                            .map(m -> m.get("name"))
                            .map(JsonValue::isNull)
                            .orElse(false))
                    .findFirst()
                    .orElseThrow();
            Map<String, JsonValue> unmatchedObj = unmatched.asObject().orElseThrow();
            assertEquals(
                    104, unmatchedObj.get("order_id").asNumber().orElseThrow().intValue());
            assertEquals("Paris", unmatchedObj.get("ship_city").asString().orElseThrow());
        }

        @Test
        void deeplyNestedFieldInJoin() {
            String sql = """
                    SELECT u.name AS name, u.address.geo.lat AS lat
                    FROM users u JOIN orders o ON u.id = o.user_id""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // Alice(2 orders) + Bob(1 order) = 3 rows
            assertEquals(3, arr.size());

            // Alice's lat should be 41.0
            double aliceLat = arr.stream()
                    .filter(v -> v.asObject()
                            .map(m -> m.get("name"))
                            .flatMap(JsonValue::asString)
                            .map("Alice"::equals)
                            .orElse(false))
                    .findFirst()
                    .orElseThrow()
                    .asObject()
                    .orElseThrow()
                    .get("lat")
                    .asNumber()
                    .orElseThrow()
                    .doubleValue();
            assertEquals(41.0, aliceLat, 0.001);
        }

        @Test
        void innerJoinNestedWithAggregate() {
            String sql = """
                    SELECT u.address.country AS country, SUM(o.total) AS total_sum
                    FROM users u JOIN orders o ON u.id = o.user_id
                    GROUP BY u.address.country
                    ORDER BY total_sum DESC""";
            JsonValue result = SQL4Json.queryAsJsonValue(sql, DATA);
            List<JsonValue> arr = result.asArray().orElseThrow();
            // TR (Alice: 250+75=325) and DE (Bob: 180) → 2 groups
            assertEquals(2, arr.size());

            // TR should be first (325 > 180)
            Map<String, JsonValue> first = arr.getFirst().asObject().orElseThrow();
            assertEquals("TR", first.get("country").asString().orElseThrow());
            assertEquals(325.0, first.get("total_sum").asNumber().orElseThrow().doubleValue(), 0.001);

            Map<String, JsonValue> second = arr.get(1).asObject().orElseThrow();
            assertEquals("DE", second.get("country").asString().orElseThrow());
            assertEquals(180.0, second.get("total_sum").asNumber().orElseThrow().doubleValue(), 0.001);
        }
    }
}
