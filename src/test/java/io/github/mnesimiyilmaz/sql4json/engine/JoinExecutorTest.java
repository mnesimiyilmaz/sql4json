// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mnesimiyilmaz.sql4json.parser.JoinDef;
import io.github.mnesimiyilmaz.sql4json.parser.JoinEquality;
import io.github.mnesimiyilmaz.sql4json.parser.JoinType;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JoinExecutorTest {

    private static final int NO_LIMIT = Integer.MAX_VALUE;

    private FieldKey.Interner interner;

    @BeforeEach
    void setUp() {
        interner = new FieldKey.Interner();
    }

    /** Build a {@link RowSchema} from the alias-prefixed field names in source order. */
    private RowSchema schema(String alias, String... fields) {
        var keys = new ArrayList<FieldKey>(fields.length);
        for (String f : fields) keys.add(FieldKey.of(alias + "." + f, interner));
        return RowSchema.of(keys);
    }

    /** Build a single {@link FlatRow} bound to the given schema, populating slots by alias-prefixed field name. */
    private FlatRow row(RowSchema schema, String alias, Map<String, SqlValue> fields) {
        Object[] vals = new Object[schema.size()];
        fields.forEach((k, v) -> {
            int idx = schema.indexOf(FieldKey.of(alias + "." + k, interner));
            if (idx >= 0) vals[idx] = v;
        });
        return FlatRow.of(schema, vals);
    }

    /** Convenience: build a single-row LinkedHashMap to preserve insertion order for clearer test reads. */
    private static Map<String, SqlValue> fields(String k1, SqlValue v1) {
        var m = new LinkedHashMap<String, SqlValue>();
        m.put(k1, v1);
        return m;
    }

    private static Map<String, SqlValue> fields(String k1, SqlValue v1, String k2, SqlValue v2) {
        var m = new LinkedHashMap<String, SqlValue>();
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }

    private static Map<String, SqlValue> fields(
            String k1, SqlValue v1, String k2, SqlValue v2, String k3, SqlValue v3) {
        var m = new LinkedHashMap<String, SqlValue>();
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        return m;
    }

    @Test
    void inner_join_matches_on_key() {
        RowSchema leftSchema = schema("u", "id", "name");
        RowSchema rightSchema = schema("o", "user_id", "amount");
        RowSchema mergedSchema = leftSchema.concat(rightSchema);

        var left = List.of(
                row(leftSchema, "u", fields("id", SqlNumber.of(1), "name", new SqlString("Alice"))),
                row(leftSchema, "u", fields("id", SqlNumber.of(2), "name", new SqlString("Bob"))));
        var right = List.of(row(rightSchema, "o", fields("user_id", SqlNumber.of(1), "amount", SqlNumber.of(100))));

        var joinDef = new JoinDef("orders", "o", JoinType.INNER, List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, mergedSchema, joinDef, NO_LIMIT);

        assertEquals(1, result.size());
        assertEquals(new SqlString("Alice"), result.getFirst().get(FieldKey.of("u.name")));
        assertEquals(SqlNumber.of(100), result.getFirst().get(FieldKey.of("o.amount")));
    }

    @Test
    void inner_join_no_matches_returns_empty() {
        RowSchema leftSchema = schema("u", "id");
        RowSchema rightSchema = schema("o", "user_id");
        RowSchema mergedSchema = leftSchema.concat(rightSchema);

        var left = List.of(row(leftSchema, "u", fields("id", SqlNumber.of(99))));
        var right = List.of(row(rightSchema, "o", fields("user_id", SqlNumber.of(1))));

        var joinDef = new JoinDef("orders", "o", JoinType.INNER, List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, mergedSchema, joinDef, NO_LIMIT);
        assertTrue(result.isEmpty());
    }

    @Test
    void inner_join_multiple_matches() {
        RowSchema leftSchema = schema("u", "id", "name");
        RowSchema rightSchema = schema("o", "user_id", "amount");
        RowSchema mergedSchema = leftSchema.concat(rightSchema);

        var left = List.of(row(leftSchema, "u", fields("id", SqlNumber.of(1), "name", new SqlString("Alice"))));
        var right = List.of(
                row(rightSchema, "o", fields("user_id", SqlNumber.of(1), "amount", SqlNumber.of(100))),
                row(rightSchema, "o", fields("user_id", SqlNumber.of(1), "amount", SqlNumber.of(200))));

        var joinDef = new JoinDef("orders", "o", JoinType.INNER, List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, mergedSchema, joinDef, NO_LIMIT);
        assertEquals(2, result.size());
    }

    @Test
    void left_join_preserves_unmatched_left_rows() {
        RowSchema leftSchema = schema("u", "id", "name");
        RowSchema rightSchema = schema("o", "user_id", "amount");
        RowSchema mergedSchema = leftSchema.concat(rightSchema);

        var left = List.of(
                row(leftSchema, "u", fields("id", SqlNumber.of(1), "name", new SqlString("Alice"))),
                row(leftSchema, "u", fields("id", SqlNumber.of(2), "name", new SqlString("Bob"))));
        var right = List.of(row(rightSchema, "o", fields("user_id", SqlNumber.of(1), "amount", SqlNumber.of(100))));

        var joinDef = new JoinDef("orders", "o", JoinType.LEFT, List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, mergedSchema, joinDef, NO_LIMIT);

        assertEquals(2, result.size());
        var bobRow = result.get(1);
        assertEquals(new SqlString("Bob"), bobRow.get(FieldKey.of("u.name")));
        assertTrue(bobRow.get(FieldKey.of("o.amount")).isNull());
        assertTrue(bobRow.get(FieldKey.of("o.user_id")).isNull());
    }

    @Test
    void right_join_preserves_unmatched_right_rows() {
        RowSchema leftSchema = schema("u", "id", "name");
        RowSchema rightSchema = schema("o", "user_id", "amount");
        // Merged schema is leftSchema.concat(rightSchema) regardless of direction.
        RowSchema mergedSchema = leftSchema.concat(rightSchema);

        var left = List.of(row(leftSchema, "u", fields("id", SqlNumber.of(1), "name", new SqlString("Alice"))));
        var right = List.of(
                row(rightSchema, "o", fields("user_id", SqlNumber.of(1), "amount", SqlNumber.of(100))),
                row(rightSchema, "o", fields("user_id", SqlNumber.of(99), "amount", SqlNumber.of(50))));

        var joinDef = new JoinDef("orders", "o", JoinType.RIGHT, List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, mergedSchema, joinDef, NO_LIMIT);

        assertEquals(2, result.size());
        var unmatchedRow = result.stream()
                .filter(r -> r.get(FieldKey.of("o.amount")).equals(SqlNumber.of(50)))
                .findFirst()
                .orElseThrow();
        assertTrue(unmatchedRow.get(FieldKey.of("u.name")).isNull());
    }

    @Test
    void multi_column_on_condition() {
        RowSchema leftSchema = schema("a", "x", "y");
        RowSchema rightSchema = schema("b", "x", "y", "val");
        RowSchema mergedSchema = leftSchema.concat(rightSchema);

        var left = List.of(
                row(leftSchema, "a", fields("x", SqlNumber.of(1), "y", SqlNumber.of(10))),
                row(leftSchema, "a", fields("x", SqlNumber.of(1), "y", SqlNumber.of(20))));
        var right = List.of(row(
                rightSchema, "b", fields("x", SqlNumber.of(1), "y", SqlNumber.of(10), "val", new SqlString("match"))));

        var joinDef = new JoinDef(
                "b", "b", JoinType.INNER, List.of(new JoinEquality("a.x", "b.x"), new JoinEquality("a.y", "b.y")));

        var result = JoinExecutor.execute(left, right, mergedSchema, joinDef, NO_LIMIT);

        assertEquals(1, result.size());
        assertEquals(new SqlString("match"), result.getFirst().get(FieldKey.of("b.val")));
    }

    @Test
    void left_join_empty_right_returns_all_left_with_nulls() {
        RowSchema leftSchema = schema("u", "id");
        RowSchema rightSchema = schema("o", "user_id");
        RowSchema mergedSchema = leftSchema.concat(rightSchema);

        var left = List.of(row(leftSchema, "u", fields("id", SqlNumber.of(1))));
        List<FlatRow> right = List.of();

        var joinDef = new JoinDef("orders", "o", JoinType.LEFT, List.of(new JoinEquality("u.id", "o.user_id")));

        var result = JoinExecutor.execute(left, right, mergedSchema, joinDef, NO_LIMIT);
        assertEquals(1, result.size());
        assertTrue(result.getFirst().get(FieldKey.of("o.user_id")).isNull());
    }
}
