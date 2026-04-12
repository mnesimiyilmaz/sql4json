package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.parser.JoinDef;
import io.github.mnesimiyilmaz.sql4json.parser.JoinEquality;
import io.github.mnesimiyilmaz.sql4json.parser.JoinType;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinExecutorTest {

    private static final int NO_LIMIT = Integer.MAX_VALUE;

    private FieldKey.Interner interner;

    @BeforeEach
    void setUp() {
        interner = new FieldKey.Interner();
    }

    // Helper: create a prefixed eager row
    private Row prefixedRow(String alias, Map<String, SqlValue> fields) {
        var prefixed = new HashMap<FieldKey, SqlValue>();
        fields.forEach((k, v) -> prefixed.put(FieldKey.of(alias + "." + k, interner), v));
        return Row.eager(prefixed);
    }

    @Test
    void inner_join_matches_on_key() {
        var left = List.of(
                prefixedRow("u", Map.of("id", SqlNumber.of(1), "name", new SqlString("Alice"))),
                prefixedRow("u", Map.of("id", SqlNumber.of(2), "name", new SqlString("Bob"))));
        var right = List.of(
                prefixedRow("o", Map.of("user_id", SqlNumber.of(1), "amount", SqlNumber.of(100))));
        Set<FieldKey> rightSchema = Set.of(
                FieldKey.of("o.user_id", interner), FieldKey.of("o.amount", interner));

        var joinDef = new JoinDef("orders", "o", JoinType.INNER,
                List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, rightSchema, joinDef, NO_LIMIT);

        assertEquals(1, result.size());
        assertEquals(new SqlString("Alice"), result.getFirst().get(FieldKey.of("u.name")));
        assertEquals(SqlNumber.of(100), result.getFirst().get(FieldKey.of("o.amount")));
    }

    @Test
    void inner_join_no_matches_returns_empty() {
        var left = List.of(
                prefixedRow("u", Map.of("id", SqlNumber.of(99))));
        var right = List.of(
                prefixedRow("o", Map.of("user_id", SqlNumber.of(1))));
        Set<FieldKey> rightSchema = Set.of(FieldKey.of("o.user_id", interner));

        var joinDef = new JoinDef("orders", "o", JoinType.INNER,
                List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, rightSchema, joinDef, NO_LIMIT);
        assertTrue(result.isEmpty());
    }

    @Test
    void inner_join_multiple_matches() {
        var left = List.of(
                prefixedRow("u", Map.of("id", SqlNumber.of(1), "name", new SqlString("Alice"))));
        var right = List.of(
                prefixedRow("o", Map.of("user_id", SqlNumber.of(1), "amount", SqlNumber.of(100))),
                prefixedRow("o", Map.of("user_id", SqlNumber.of(1), "amount", SqlNumber.of(200))));
        Set<FieldKey> rightSchema = Set.of(
                FieldKey.of("o.user_id", interner), FieldKey.of("o.amount", interner));

        var joinDef = new JoinDef("orders", "o", JoinType.INNER,
                List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, rightSchema, joinDef, NO_LIMIT);
        assertEquals(2, result.size());
    }

    @Test
    void left_join_preserves_unmatched_left_rows() {
        var left = List.of(
                prefixedRow("u", Map.of("id", SqlNumber.of(1), "name", new SqlString("Alice"))),
                prefixedRow("u", Map.of("id", SqlNumber.of(2), "name", new SqlString("Bob"))));
        var right = List.of(
                prefixedRow("o", Map.of("user_id", SqlNumber.of(1), "amount", SqlNumber.of(100))));
        Set<FieldKey> rightSchema = Set.of(
                FieldKey.of("o.user_id", interner), FieldKey.of("o.amount", interner));

        var joinDef = new JoinDef("orders", "o", JoinType.LEFT,
                List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, rightSchema, joinDef, NO_LIMIT);

        assertEquals(2, result.size());
        var bobRow = result.get(1);
        assertEquals(new SqlString("Bob"), bobRow.get(FieldKey.of("u.name")));
        assertTrue(bobRow.get(FieldKey.of("o.amount")).isNull());
        assertTrue(bobRow.get(FieldKey.of("o.user_id")).isNull());
    }

    @Test
    void right_join_preserves_unmatched_right_rows() {
        var left = List.of(
                prefixedRow("u", Map.of("id", SqlNumber.of(1), "name", new SqlString("Alice"))));
        var right = List.of(
                prefixedRow("o", Map.of("user_id", SqlNumber.of(1), "amount", SqlNumber.of(100))),
                prefixedRow("o", Map.of("user_id", SqlNumber.of(99), "amount", SqlNumber.of(50))));
        Set<FieldKey> leftSchema = Set.of(
                FieldKey.of("u.id", interner), FieldKey.of("u.name", interner));

        var joinDef = new JoinDef("orders", "o", JoinType.RIGHT,
                List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, leftSchema, joinDef, NO_LIMIT);

        assertEquals(2, result.size());
        var unmatchedRow = result.stream()
                .filter(r -> r.get(FieldKey.of("o.amount")).equals(SqlNumber.of(50)))
                .findFirst().orElseThrow();
        assertTrue(unmatchedRow.get(FieldKey.of("u.name")).isNull());
    }

    @Test
    void multi_column_on_condition() {
        var left = List.of(
                prefixedRow("a", Map.of("x", SqlNumber.of(1), "y", SqlNumber.of(10))),
                prefixedRow("a", Map.of("x", SqlNumber.of(1), "y", SqlNumber.of(20))));
        var right = List.of(
                prefixedRow("b", Map.of("x", SqlNumber.of(1), "y", SqlNumber.of(10), "val", new SqlString("match"))));
        Set<FieldKey> rightSchema = Set.of(
                FieldKey.of("b.x", interner), FieldKey.of("b.y", interner), FieldKey.of("b.val", interner));

        var joinDef = new JoinDef("b", "b", JoinType.INNER, List.of(
                new JoinEquality("a.x", "b.x"),
                new JoinEquality("a.y", "b.y")));

        var result = JoinExecutor.execute(left, right, rightSchema, joinDef, NO_LIMIT);

        assertEquals(1, result.size());
        assertEquals(new SqlString("match"), result.getFirst().get(FieldKey.of("b.val")));
    }

    @Test
    void left_join_empty_right_returns_all_left_with_nulls() {
        var left = List.of(
                prefixedRow("u", Map.of("id", SqlNumber.of(1))));
        List<Row> right = List.of();
        Set<FieldKey> rightSchema = Set.of(FieldKey.of("o.user_id", interner));

        var joinDef = new JoinDef("orders", "o", JoinType.LEFT,
                List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, rightSchema, joinDef, NO_LIMIT);
        assertEquals(1, result.size());
        assertTrue(result.getFirst().get(FieldKey.of("o.user_id")).isNull());
    }
}
