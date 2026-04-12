package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.ColumnRef;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.json.JsonNumberValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionHandlerTest {

    // ── NullCheckConditionHandler ────────────────────────────────────────

    @Nested
    class NullCheckTests {

        private final NullCheckConditionHandler handler = new NullCheckConditionHandler();

        private static Row rowWithString(String field, String value) {
            var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
            fields.put(field, new JsonStringValue(value));
            return Row.lazy(new JsonObjectValue(fields), new FieldKey.Interner());
        }

        private static Row rowWithNull() {
            var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
            // field absent — Row.get() returns SqlNull for absent keys
            return Row.lazy(new JsonObjectValue(fields), new FieldKey.Interner());
        }

        private static ConditionContext isNullCtx(String col) {
            return new ConditionContext(ConditionContext.ConditionType.IS_NULL,
                    new ColumnRef(col), null, null, null, null, null, null);
        }

        private static ConditionContext isNotNullCtx(String col) {
            return new ConditionContext(ConditionContext.ConditionType.IS_NOT_NULL,
                    new ColumnRef(col), null, null, null, null, null, null);
        }

        // ── canHandle ───────────────────────────────────────────────────

        @Test
        void canHandle_isNull_true() {
            assertTrue(handler.canHandle(isNullCtx("email")));
        }

        @Test
        void canHandle_isNotNull_true() {
            assertTrue(handler.canHandle(isNotNullCtx("email")));
        }

        @Test
        void canHandle_comparison_false() {
            ConditionContext ctx = new ConditionContext(ConditionContext.ConditionType.COMPARISON,
                    new ColumnRef("age"), ">", SqlNumber.of(1),
                    new Expression.LiteralVal(SqlNumber.of(1)), null, null, null);
            assertFalse(handler.canHandle(ctx));
        }

        @Test
        void canHandle_like_false() {
            ConditionContext ctx = new ConditionContext(ConditionContext.ConditionType.LIKE,
                    new ColumnRef("name"), "LIKE", new SqlString("A%"),
                    new Expression.LiteralVal(new SqlString("A%")), null, null, null);
            assertFalse(handler.canHandle(ctx));
        }

        // ── IS NULL: absent field resolves to SqlNull ────────────────────

        @Test
        void isNull_absentField_returnsTrue() {
            CriteriaNode node = handler.handle(isNullCtx("email"), null, null);
            Row row = rowWithNull(); // "email" absent → SqlNull
            assertTrue(node.test(row));
        }

        @Test
        void isNull_presentField_returnsFalse() {
            CriteriaNode node = handler.handle(isNullCtx("email"), null, null);
            Row row = rowWithString("email", "alice@example.com");
            assertFalse(node.test(row));
        }

        @Test
        void isNull_explicitSqlNullInEagerRow_returnsTrue() {
            CriteriaNode node = handler.handle(isNullCtx("opt"), null, null);
            FieldKey optKey = FieldKey.of("opt");
            Row row = Row.eager(Map.of(optKey, SqlNull.INSTANCE));
            assertTrue(node.test(row));
        }

        // ── IS NOT NULL ─────────────────────────────────────────────────

        @Test
        void isNotNull_presentField_returnsTrue() {
            CriteriaNode node = handler.handle(isNotNullCtx("name"), null, null);
            Row row = rowWithString("name", "Alice");
            assertTrue(node.test(row));
        }

        @Test
        void isNotNull_absentField_returnsFalse() {
            CriteriaNode node = handler.handle(isNotNullCtx("email"), null, null);
            Row row = rowWithNull(); // "email" absent → SqlNull
            assertFalse(node.test(row));
        }

        @Test
        void isNotNull_explicitSqlNullInEagerRow_returnsFalse() {
            CriteriaNode node = handler.handle(isNotNullCtx("opt"), null, null);
            FieldKey optKey = FieldKey.of("opt");
            Row row = Row.eager(Map.of(optKey, SqlNull.INSTANCE));
            assertFalse(node.test(row));
        }

        // ── Numeric value (IS NOT NULL) ─────────────────────────────────

        @Test
        void isNotNull_numericValue_returnsTrue() {
            CriteriaNode node = handler.handle(isNotNullCtx("age"), null, null);
            FieldKey ageKey = FieldKey.of("age");
            Row row = Row.eager(Map.of(ageKey, SqlNumber.of(25)));
            assertTrue(node.test(row));
        }
    }

    // ── ComparisonConditionHandler ──────────────────────────────────────

    @Nested
    class ComparisonTests {

        private final FunctionRegistry           fns     = FunctionRegistry.createDefault();
        private final OperatorRegistry           ops     = OperatorRegistry.createDefault();
        private final ComparisonConditionHandler handler = new ComparisonConditionHandler();

        private static Row rowWithNumber(String field, int value) {
            var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
            fields.put(field, new JsonNumberValue(value));
            return Row.lazy(new JsonObjectValue(fields), new FieldKey.Interner());
        }

        private static Row rowWithString(String field, String value) {
            var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
            fields.put(field, new JsonStringValue(value));
            return Row.lazy(new JsonObjectValue(fields), new FieldKey.Interner());
        }

        private static ConditionContext comparisonCtx(String col, String op, SqlValue val) {
            return new ConditionContext(ConditionContext.ConditionType.COMPARISON,
                    new ColumnRef(col), op, val, new Expression.LiteralVal(val), null, null, null);
        }

        private static ConditionContext comparisonCtxWithFn(String col, String op, SqlValue val,
                                                            String fnName) {
            Expression lhs = new Expression.ScalarFnCall(fnName, List.of(new ColumnRef(col)));
            return new ConditionContext(ConditionContext.ConditionType.COMPARISON,
                    lhs, op, val, new Expression.LiteralVal(val), null, null, null);
        }

        // ── canHandle ───────────────────────────────────────────────────

        @Test
        void canHandle_comparison_true() {
            ConditionContext ctx = comparisonCtx("age", ">", SqlNumber.of(18));
            assertTrue(handler.canHandle(ctx));
        }

        @Test
        void canHandle_like_false() {
            ConditionContext ctx = new ConditionContext(ConditionContext.ConditionType.LIKE,
                    new ColumnRef("name"), "LIKE", new SqlString("A%"),
                    new Expression.LiteralVal(new SqlString("A%")), null, null, null);
            assertFalse(handler.canHandle(ctx));
        }

        @Test
        void canHandle_isNull_false() {
            ConditionContext ctx = new ConditionContext(ConditionContext.ConditionType.IS_NULL,
                    new ColumnRef("name"), null, null, null, null, null, null);
            assertFalse(handler.canHandle(ctx));
        }

        // ── handle: basic comparison ────────────────────────────────────

        @Test
        void handle_equals_number_match() {
            CriteriaNode node = handler.handle(comparisonCtx("age", "=", SqlNumber.of(25)), ops, fns);
            assertTrue(node.test(rowWithNumber("age", 25)));
        }

        @Test
        void handle_equals_number_noMatch() {
            CriteriaNode node = handler.handle(comparisonCtx("age", "=", SqlNumber.of(25)), ops, fns);
            assertFalse(node.test(rowWithNumber("age", 30)));
        }

        @Test
        void handle_greaterThan_number() {
            CriteriaNode node = handler.handle(comparisonCtx("age", ">", SqlNumber.of(20)), ops, fns);
            assertTrue(node.test(rowWithNumber("age", 25)));
            assertFalse(node.test(rowWithNumber("age", 15)));
        }

        @Test
        void handle_lessThan_number() {
            CriteriaNode node = handler.handle(comparisonCtx("age", "<", SqlNumber.of(30)), ops, fns);
            assertTrue(node.test(rowWithNumber("age", 25)));
            assertFalse(node.test(rowWithNumber("age", 35)));
        }

        @Test
        void handle_greaterThanOrEqual_exactBoundary() {
            CriteriaNode node = handler.handle(comparisonCtx("score", ">=", SqlNumber.of(50)), ops, fns);
            assertTrue(node.test(rowWithNumber("score", 50)));
            assertTrue(node.test(rowWithNumber("score", 51)));
            assertFalse(node.test(rowWithNumber("score", 49)));
        }

        @Test
        void handle_equals_string() {
            CriteriaNode node = handler.handle(comparisonCtx("status", "=", new SqlString("active")), ops, fns);
            assertTrue(node.test(rowWithString("status", "active")));
            assertFalse(node.test(rowWithString("status", "inactive")));
        }

        // ── handle: missing column → SqlNull → comparison false ─────────

        @Test
        void handle_missingColumn_returnsFalse() {
            CriteriaNode node = handler.handle(comparisonCtx("nonexistent", "=", SqlNumber.of(1)), ops, fns);
            Row row = rowWithNumber("age", 25); // "nonexistent" not present → SqlNull
            assertFalse(node.test(row));
        }

        // ── handle: with scalar function on column ──────────────────────

        @Test
        void handle_lowerFunction_caseInsensitiveMatch() {
            // WHERE LOWER(name) = 'alice'  → matches "Alice", "ALICE", "alice"
            ConditionContext ctx = comparisonCtxWithFn("name", "=", new SqlString("alice"), "lower");
            CriteriaNode node = handler.handle(ctx, ops, fns);
            assertTrue(node.test(rowWithString("name", "Alice")));
            assertTrue(node.test(rowWithString("name", "ALICE")));
            assertTrue(node.test(rowWithString("name", "alice")));
            assertFalse(node.test(rowWithString("name", "Bob")));
        }

        @Test
        void handle_upperFunction() {
            // WHERE UPPER(dept) = 'ENGINEERING' → matches "engineering", "Engineering"
            ConditionContext ctx = comparisonCtxWithFn("dept", "=", new SqlString("ENGINEERING"), "upper");
            CriteriaNode node = handler.handle(ctx, ops, fns);
            assertTrue(node.test(rowWithString("dept", "engineering")));
            assertTrue(node.test(rowWithString("dept", "Engineering")));
        }

        @Test
        void handle_notEquals_number_match() {
            CriteriaNode node = handler.handle(comparisonCtx("age", "!=", SqlNumber.of(25)), ops, fns);
            assertTrue(node.test(rowWithNumber("age", 30)));
        }

        @Test
        void handle_notEquals_number_noMatch() {
            CriteriaNode node = handler.handle(comparisonCtx("age", "!=", SqlNumber.of(25)), ops, fns);
            assertFalse(node.test(rowWithNumber("age", 25)));
        }

        @Test
        void handle_notEquals_string() {
            CriteriaNode node = handler.handle(comparisonCtx("status", "!=", new SqlString("active")), ops, fns);
            assertTrue(node.test(rowWithString("status", "inactive")));
            assertFalse(node.test(rowWithString("status", "active")));
        }

        @Test
        void handle_lessThanOrEqual_exactBoundary() {
            CriteriaNode node = handler.handle(comparisonCtx("score", "<=", SqlNumber.of(50)), ops, fns);
            assertTrue(node.test(rowWithNumber("score", 50)));
            assertTrue(node.test(rowWithNumber("score", 49)));
            assertFalse(node.test(rowWithNumber("score", 51)));
        }
    }
}
