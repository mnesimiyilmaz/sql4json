// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.parser;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.ColumnRef;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ParserValueObjectTest {

    // ── OrderByColumnDef ────────────────────────────────────────────────

    @Nested
    class OrderByColumnDefTests {

        @Test
        void defaults_to_ASC_when_direction_omitted() {
            var def = OrderByColumnDef.of("name");
            assertEquals("name", def.columnName());
            assertEquals("ASC", def.direction());
        }

        @Test
        void stores_explicit_direction() {
            var def = OrderByColumnDef.of("age", "DESC");
            assertEquals("age", def.columnName());
            assertEquals("DESC", def.direction());
        }
    }

    // ── QueryDefinition ─────────────────────────────────────────────────

    @Nested
    class QueryDefinitionTests {

        @Test
        void isSelectAll_true_for_single_asterisk_column() {
            var qd = new QueryDefinition(
                    List.of(SelectColumnDef.asterisk()),
                    "$r",
                    null,
                    null,
                    null,
                    null,
                    null,
                    Set.of(),
                    false,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    0,
                    Set.of(),
                    0,
                    List.of());
            assertTrue(qd.isSelectAll());
        }

        @Test
        void isSelectAll_false_for_named_column() {
            var qd = new QueryDefinition(
                    List.of(SelectColumnDef.column("name")),
                    "$r",
                    null,
                    null,
                    null,
                    null,
                    null,
                    Set.of(),
                    false,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    0,
                    Set.of(),
                    0,
                    List.of());
            assertFalse(qd.isSelectAll());
        }

        @Test
        void requiresFullFlatten_true_when_groupBy_present() {
            var qd = new QueryDefinition(
                    List.of(SelectColumnDef.column("dept")),
                    "$r",
                    null,
                    null,
                    List.<Expression>of(new ColumnRef("dept")),
                    null,
                    null,
                    Set.of(),
                    false,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    0,
                    Set.of(),
                    0,
                    List.of());
            assertTrue(qd.requiresFullFlatten());
        }

        @Test
        void requiresFullFlatten_false_without_groupBy() {
            var qd = new QueryDefinition(
                    List.of(SelectColumnDef.asterisk()),
                    "$r",
                    null,
                    null,
                    null,
                    null,
                    null,
                    Set.of(),
                    false,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    0,
                    Set.of(),
                    0,
                    List.of());
            assertFalse(qd.requiresFullFlatten());
        }

        @Test
        void fromSubQuery_null_for_simple_root_path() {
            var qd = new QueryDefinition(
                    List.of(SelectColumnDef.asterisk()),
                    "$r",
                    null,
                    null,
                    null,
                    null,
                    null,
                    Set.of(),
                    false,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    0,
                    Set.of(),
                    0,
                    List.of());
            assertNull(qd.fromSubQuery());
            assertEquals("$r", qd.rootPath());
        }
    }

    // ── JoinDef ─────────────────────────────────────────────────────────

    @Nested
    class JoinDefTests {

        @Test
        void joinType_values() {
            assertEquals(3, JoinType.values().length);
            assertNotNull(JoinType.INNER);
            assertNotNull(JoinType.LEFT);
            assertNotNull(JoinType.RIGHT);
        }

        @Test
        void joinEquality_stores_paths() {
            var eq = new JoinEquality("u.id", "o.user_id");
            assertEquals("u.id", eq.leftPath());
            assertEquals("o.user_id", eq.rightPath());
        }

        @Test
        void joinDef_stores_all_fields() {
            var eq = new JoinEquality("u.id", "o.user_id");
            var joinDef = new JoinDef("orders", "o", JoinType.INNER, List.of(eq));
            assertEquals("orders", joinDef.tableName());
            assertEquals("o", joinDef.alias());
            assertEquals(JoinType.INNER, joinDef.joinType());
            assertEquals(1, joinDef.onConditions().size());
        }

        @Test
        void joinDef_onConditions_is_immutable() {
            var eq = new JoinEquality("u.id", "o.user_id");
            var joinDef = new JoinDef("orders", "o", JoinType.LEFT, List.of(eq));
            var conditions = joinDef.onConditions();
            assertThrows(UnsupportedOperationException.class, () -> conditions.add(new JoinEquality("a", "b")));
        }

        @Test
        void joinDef_multi_column_on() {
            var joinDef = new JoinDef(
                    "b", "b", JoinType.INNER, List.of(new JoinEquality("a.x", "b.x"), new JoinEquality("a.y", "b.y")));
            assertEquals(2, joinDef.onConditions().size());
        }
    }
}
