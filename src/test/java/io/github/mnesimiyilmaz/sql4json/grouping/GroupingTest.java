package io.github.mnesimiyilmaz.sql4json.grouping;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GroupingTest {

    // ── GroupAggregator ─────────────────────────────────────────────────

    @Nested
    class GroupAggregatorTests {

        private final FunctionRegistry fn = FunctionRegistry.createDefault();

        @Test
        void count_stored_under_alias_key() {
            var row1 = Row.eager(Map.of(FieldKey.of("dept"), new SqlString("IT"),
                    FieldKey.of("name"), new SqlString("Alice")));
            var row2 = Row.eager(Map.of(FieldKey.of("dept"), new SqlString("IT"),
                    FieldKey.of("name"), new SqlString("Bob")));
            var columns = List.of(
                    SelectColumnDef.column("dept"),
                    SelectColumnDef.aggregate("COUNT", "name", "cnt")
            );

            Row result = GroupAggregator.aggregate(List.of(row1, row2), columns, fn);

            // Aggregate stored under alias "cnt"
            assertEquals(SqlNumber.of(2), result.get(FieldKey.of("cnt")));
            // Non-aggregate stored under column name "dept"
            assertEquals(new SqlString("IT"), result.get(FieldKey.of("dept")));
            assertTrue(result.isModified());
        }

        @Test
        void sum_stored_under_alias_key() {
            var row1 = Row.eager(Map.of(FieldKey.of("amount"), SqlNumber.of(10.0)));
            var row2 = Row.eager(Map.of(FieldKey.of("amount"), SqlNumber.of(20.0)));
            var columns = List.of(SelectColumnDef.aggregate("SUM", "amount", "total"));

            Row result = GroupAggregator.aggregate(List.of(row1, row2), columns, fn);
            // SUM stored under alias "total"
            assertEquals(SqlNumber.of(30.0), result.get(FieldKey.of("total")));
        }

        @Test
        void count_asterisk_counts_all_rows() {
            var rows = List.of(
                    Row.eager(Map.of(FieldKey.of("id"), SqlNumber.of(1))),
                    Row.eager(Map.of(FieldKey.of("id"), SqlNumber.of(2))),
                    Row.eager(Map.of(FieldKey.of("id"), SqlNumber.of(3)))
            );
            // COUNT(*) with no alias: aliasOrName() = "*"... but typically aliased in real SQL
            // Use alias "cnt" for clear test
            var columns = List.of(SelectColumnDef.aggregate("COUNT", "*", "cnt"));

            Row result = GroupAggregator.aggregate(rows, columns, fn);
            assertEquals(SqlNumber.of(3), result.get(FieldKey.of("cnt")));
        }
    }

    // ── GroupKey ─────────────────────────────────────────────────────────

    @Nested
    class GroupKeyTests {

        @Test
        void same_values_are_equal() {
            var list1 = List.<SqlValue>of(new SqlString("IT"), SqlNumber.of(2024));
            var list2 = List.<SqlValue>of(new SqlString("IT"), SqlNumber.of(2024));

            assertEquals(new GroupKey(list1), new GroupKey(list2));
            assertEquals(new GroupKey(list1).hashCode(), new GroupKey(list2).hashCode());
        }

        @Test
        void different_values_are_not_equal() {
            assertNotEquals(
                    new GroupKey(List.of(new SqlString("IT"))),
                    new GroupKey(List.of(new SqlString("HR"))));
        }

        @Test
        void mutating_original_list_does_not_affect_key() {
            var mutable = new ArrayList<SqlValue>();
            mutable.add(new SqlString("IT"));
            var key = new GroupKey(mutable);
            mutable.set(0, new SqlString("HR"));
            assertEquals(new GroupKey(List.of(new SqlString("IT"))), key);
        }
    }
}
