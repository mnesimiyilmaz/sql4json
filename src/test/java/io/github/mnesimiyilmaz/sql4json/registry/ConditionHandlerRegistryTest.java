// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.registry;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.ColumnRef;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.json.JsonLongValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;

class ConditionHandlerRegistryTest {

    private final FunctionRegistry fns = FunctionRegistry.createDefault();
    private final OperatorRegistry ops = OperatorRegistry.createDefault();
    private final ConditionHandlerRegistry handlers = ConditionHandlerRegistry.forSettings(Sql4jsonSettings.defaults());

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static Row rowWith(String field, io.github.mnesimiyilmaz.sql4json.types.JsonValue value) {
        var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        fields.put(field, value);
        return Row.lazy(new JsonObjectValue(fields), new FieldKey.Interner());
    }

    // ── resolve: COMPARISON ──────────────────────────────────────────────────

    @Test
    void resolve_comparison_producesWorkingNode() {
        ConditionContext ctx = new ConditionContext(
                ConditionContext.ConditionType.COMPARISON,
                new ColumnRef("age"),
                ">",
                SqlNumber.of(18),
                new Expression.LiteralVal(SqlNumber.of(18)),
                null,
                null,
                null,
                null,
                null,
                null);

        CriteriaNode node = handlers.resolve(ctx);

        assertTrue(node.test(rowWith("age", new JsonLongValue(25L))));
        assertFalse(node.test(rowWith("age", new JsonLongValue(15L))));
    }

    @Test
    void resolve_comparison_equalsString() {
        ConditionContext ctx = new ConditionContext(
                ConditionContext.ConditionType.COMPARISON,
                new ColumnRef("status"),
                "=",
                new SqlString("active"),
                new Expression.LiteralVal(new SqlString("active")),
                null,
                null,
                null,
                null,
                null,
                null);

        CriteriaNode node = handlers.resolve(ctx);

        assertTrue(node.test(rowWith("status", new JsonStringValue("active"))));
        assertFalse(node.test(rowWith("status", new JsonStringValue("inactive"))));
    }

    // ── resolve: LIKE ────────────────────────────────────────────────────────

    @Test
    void resolve_like_producesWorkingNode() {
        ConditionContext ctx = new ConditionContext(
                ConditionContext.ConditionType.LIKE,
                new ColumnRef("name"),
                "LIKE",
                new SqlString("Ali%"),
                new Expression.LiteralVal(new SqlString("Ali%")),
                null,
                null,
                null,
                null,
                null,
                null);

        CriteriaNode node = handlers.resolve(ctx);

        assertTrue(node.test(rowWith("name", new JsonStringValue("Alice"))));
        assertFalse(node.test(rowWith("name", new JsonStringValue("Bob"))));
    }

    // ── resolve: IS NULL ─────────────────────────────────────────────────────

    @Test
    void resolve_isNull_producesWorkingNode() {
        ConditionContext ctx = new ConditionContext(
                ConditionContext.ConditionType.IS_NULL,
                new ColumnRef("email"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

        CriteriaNode node = handlers.resolve(ctx);

        // Row without "email" field → SqlNull → IS NULL = true
        Row rowMissingEmail = rowWith("name", new JsonStringValue("Alice"));
        assertTrue(node.test(rowMissingEmail));

        // Row with "email" present → IS NULL = false
        Row rowWithEmail = rowWith("email", new JsonStringValue("alice@ex.com"));
        assertFalse(node.test(rowWithEmail));
    }

    // ── resolve: IS NOT NULL ─────────────────────────────────────────────────

    @Test
    void resolve_isNotNull_producesWorkingNode() {
        ConditionContext ctx = new ConditionContext(
                ConditionContext.ConditionType.IS_NOT_NULL,
                new ColumnRef("phone"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

        CriteriaNode node = handlers.resolve(ctx);

        Row rowWithPhone = rowWith("phone", new JsonStringValue("555-1234"));
        assertTrue(node.test(rowWithPhone));

        Row rowMissingPhone = rowWith("name", new JsonStringValue("Bob"));
        assertFalse(node.test(rowMissingPhone));
    }

    // ── resolve: no matching handler → exception ──────────────────────────────

    @Test
    void resolve_noHandler_throwsException() {
        // Register an empty registry with no handlers
        ConditionHandlerRegistry empty = new ConditionHandlerRegistry(ops, fns);
        ConditionContext ctx = new ConditionContext(
                ConditionContext.ConditionType.COMPARISON,
                new ColumnRef("age"),
                ">",
                SqlNumber.of(1),
                new Expression.LiteralVal(SqlNumber.of(1)),
                null,
                null,
                null,
                null,
                null,
                null);
        assertThrows(SQL4JsonExecutionException.class, () -> empty.resolve(ctx));
    }

    // ── Integration: AND + OR combinations ───────────────────────────────────

    @Test
    void integration_andCombination() {
        // (age > 18) AND (status = 'active')
        ConditionContext ageCond = new ConditionContext(
                ConditionContext.ConditionType.COMPARISON,
                new ColumnRef("age"),
                ">",
                SqlNumber.of(18),
                new Expression.LiteralVal(SqlNumber.of(18)),
                null,
                null,
                null,
                null,
                null,
                null);
        ConditionContext statusCond = new ConditionContext(
                ConditionContext.ConditionType.COMPARISON,
                new ColumnRef("status"),
                "=",
                new SqlString("active"),
                new Expression.LiteralVal(new SqlString("active")),
                null,
                null,
                null,
                null,
                null,
                null);

        CriteriaNode ageNode = handlers.resolve(ageCond);
        CriteriaNode statusNode = handlers.resolve(statusCond);
        CriteriaNode combined = new AndNode(ageNode, statusNode);

        var activeAdultFields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        activeAdultFields.put("age", new JsonLongValue(25L));
        activeAdultFields.put("status", new JsonStringValue("active"));
        Row activeAdult = Row.lazy(new JsonObjectValue(activeAdultFields), new FieldKey.Interner());
        assertTrue(combined.test(activeAdult));

        var youngActiveFields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        youngActiveFields.put("age", new JsonLongValue(15L));
        youngActiveFields.put("status", new JsonStringValue("active"));
        Row youngActive = Row.lazy(new JsonObjectValue(youngActiveFields), new FieldKey.Interner());
        assertFalse(combined.test(youngActive)); // age fails
    }

    @Test
    void integration_orCombination() {
        // (name LIKE 'Al%') OR (age > 50)
        ConditionContext likeCond = new ConditionContext(
                ConditionContext.ConditionType.LIKE,
                new ColumnRef("name"),
                "LIKE",
                new SqlString("Al%"),
                new Expression.LiteralVal(new SqlString("Al%")),
                null,
                null,
                null,
                null,
                null,
                null);
        ConditionContext ageCond = new ConditionContext(
                ConditionContext.ConditionType.COMPARISON,
                new ColumnRef("age"),
                ">",
                SqlNumber.of(50),
                new Expression.LiteralVal(SqlNumber.of(50)),
                null,
                null,
                null,
                null,
                null,
                null);

        CriteriaNode combined = new OrNode(handlers.resolve(likeCond), handlers.resolve(ageCond));

        // Matches via name
        Row row1 = rowWith("name", new JsonStringValue("Alice"));
        assertTrue(combined.test(row1));

        // Matches via age — needs both fields
        var elderFields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        elderFields.put("name", new JsonStringValue("Bob"));
        elderFields.put("age", new JsonLongValue(60L));
        Row elder = Row.lazy(new JsonObjectValue(elderFields), new FieldKey.Interner());
        assertTrue(combined.test(elder));

        // Neither matches
        var youngBobFields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        youngBobFields.put("name", new JsonStringValue("Bob"));
        youngBobFields.put("age", new JsonLongValue(25L));
        Row youngBob = Row.lazy(new JsonObjectValue(youngBobFields), new FieldKey.Interner());
        assertFalse(combined.test(youngBob));
    }

    @Test
    void integration_nullCheck_withLike() {
        // IS NOT NULL AND name LIKE 'A%'
        ConditionContext nullCheck = new ConditionContext(
                ConditionContext.ConditionType.IS_NOT_NULL,
                new ColumnRef("name"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        ConditionContext likeCond = new ConditionContext(
                ConditionContext.ConditionType.LIKE,
                new ColumnRef("name"),
                "LIKE",
                new SqlString("A%"),
                new Expression.LiteralVal(new SqlString("A%")),
                null,
                null,
                null,
                null,
                null,
                null);

        CriteriaNode combined = new AndNode(handlers.resolve(nullCheck), handlers.resolve(likeCond));

        assertTrue(combined.test(rowWith("name", new JsonStringValue("Alice"))));
        assertFalse(combined.test(rowWith("name", new JsonStringValue("Bob"))));
        assertFalse(combined.test(rowWith("other", new JsonStringValue("Alice")))); // name absent
    }
}
