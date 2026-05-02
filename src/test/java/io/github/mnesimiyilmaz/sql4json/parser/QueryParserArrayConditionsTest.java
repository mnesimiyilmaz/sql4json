// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.parser;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.registry.*;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import org.junit.jupiter.api.Test;

/**
 * Parse-only smoke tests for the array-search predicate grammar additions: {@code CONTAINS}, {@code @>}, {@code <@},
 * {@code &&}, and {@code ARRAY[...]} literals on the right-hand side of comparisons. The tests verify that the ANTLR
 * grammar plus listener accept these constructs without throwing.
 */
class QueryParserArrayConditionsTest {

    private static final Sql4jsonSettings DEFAULTS = Sql4jsonSettings.defaults();

    private static void assertParsesWithoutParseException(String sql) {
        try {
            QueryParser.parse(sql, DEFAULTS);
        } catch (SQL4JsonParseException e) {
            fail("Expected SQL to parse without SQL4JsonParseException, but got: " + e.getMessage());
        }
    }

    @Test
    void parses_contains_with_scalar_rhs() {
        assertParsesWithoutParseException("SELECT * FROM $r WHERE tags CONTAINS 'admin'");
    }

    @Test
    void parses_arrayContains_with_array_literal() {
        assertParsesWithoutParseException("SELECT * FROM $r WHERE tags @> ARRAY['admin','editor']");
    }

    @Test
    void parses_arrayContainedBy_with_array_literal() {
        assertParsesWithoutParseException("SELECT * FROM $r WHERE tags <@ ARRAY['admin']");
    }

    @Test
    void parses_arrayOverlap_with_array_literal() {
        assertParsesWithoutParseException("SELECT * FROM $r WHERE tags && ARRAY['x','y']");
    }

    @Test
    void parses_array_equality_with_literal() {
        assertParsesWithoutParseException("SELECT * FROM $r WHERE tags = ARRAY['admin','editor']");
    }

    @Test
    void parses_arrayContains_with_column_ref_rhs() {
        assertParsesWithoutParseException("SELECT * FROM $r WHERE u.tags @> required");
    }

    @Test
    void parses_arrayContains_with_named_parameter_rhs() {
        assertParsesWithoutParseException("SELECT * FROM $r WHERE tags @> :tagList");
    }

    @Test
    void parses_arrayContains_with_positional_parameter_rhs() {
        assertParsesWithoutParseException("SELECT * FROM $r WHERE tags @> ?");
    }

    @Test
    void parses_empty_array_literal() {
        assertParsesWithoutParseException("SELECT * FROM $r WHERE tags @> ARRAY[]");
    }

    @Test
    void contains_condition_carries_correct_type_and_lhs_and_rhs_literal() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags CONTAINS 'admin'", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.CONTAINS, ctx.type());
        assertInstanceOf(Expression.ColumnRef.class, ctx.lhsExpression());
        assertEquals("tags", ctx.lhsExpression().innermostColumnPath());
        assertNotNull(ctx.testValue());
        assertNull(ctx.rhsExpression()); // literal lands in testValue, not rhsExpression
    }

    @Test
    void arrayContains_with_literal_carries_valueExpressions() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags @> ARRAY['admin','editor']", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.ARRAY_CONTAINS, ctx.type());
        assertInstanceOf(Expression.ColumnRef.class, ctx.lhsExpression());
        assertNotNull(ctx.valueExpressions());
        assertEquals(2, ctx.valueExpressions().size());
        assertInstanceOf(Expression.LiteralVal.class, ctx.valueExpressions().get(0));
        assertNull(ctx.rhsExpression());
    }

    @Test
    void arrayContainedBy_with_literal_uses_correct_type() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags <@ ARRAY['admin']", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.ARRAY_CONTAINED_BY, ctx.type());
        assertEquals(1, ctx.valueExpressions().size());
    }

    @Test
    void arrayOverlap_with_literal_uses_correct_type() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags && ARRAY['a','b','c']", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.ARRAY_OVERLAP, ctx.type());
        assertEquals(3, ctx.valueExpressions().size());
    }

    @Test
    void arrayContains_with_empty_literal_carries_empty_valueExpressions() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags @> ARRAY[]", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.ARRAY_CONTAINS, ctx.type());
        assertNotNull(ctx.valueExpressions());
        assertTrue(ctx.valueExpressions().isEmpty());
    }

    @Test
    void arrayContains_with_column_ref_carries_rhsExpression() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE u.tags @> required", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.ARRAY_CONTAINS, ctx.type());
        assertInstanceOf(Expression.ColumnRef.class, ctx.rhsExpression());
        assertNull(ctx.valueExpressions());
    }

    @Test
    void arrayContains_with_named_parameter_carries_rhsExpression_as_paramRef() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags @> :tagList", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.ARRAY_CONTAINS, ctx.type());
        assertInstanceOf(Expression.ParameterRef.class, ctx.rhsExpression());
        assertNull(ctx.valueExpressions());
    }

    @Test
    void arrayOverlap_with_positional_parameter_carries_rhsExpression() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags && ?", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.ARRAY_OVERLAP, ctx.type());
        assertInstanceOf(Expression.ParameterRef.class, ctx.rhsExpression());
    }

    // T7 — equality routing
    @Test
    void equals_with_array_literal_routes_to_ARRAY_EQUALS() {
        var def = QueryParser.parse("SELECT * FROM $r WHERE tags = ARRAY['admin','editor']", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.ARRAY_EQUALS, ctx.type());
        assertEquals(2, ctx.valueExpressions().size());
    }

    @Test
    void notEquals_with_array_literal_routes_to_ARRAY_NOT_EQUALS() {
        var def = QueryParser.parse("SELECT * FROM $r WHERE tags != ARRAY['admin']", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.ARRAY_NOT_EQUALS, ctx.type());
        assertEquals(1, ctx.valueExpressions().size());
    }

    @Test
    void equals_with_scalar_literal_remains_COMPARISON() {
        var def = QueryParser.parse("SELECT * FROM $r WHERE name = 'admin'", DEFAULTS);
        var ctx = findFirstConditionContext(def);
        assertEquals(ConditionContext.ConditionType.COMPARISON, ctx.type());
    }

    // T8 — parse-time rejection
    @Test
    void lessThan_with_array_literal_throws_parseException() {
        var ex = assertThrows(
                SQL4JsonParseException.class,
                () -> QueryParser.parse("SELECT * FROM $r WHERE tags < ARRAY['a']", DEFAULTS));
        assertTrue(
                ex.getMessage().contains("does not support array right-hand side"),
                "expected message to mention array RHS rejection, was: " + ex.getMessage());
    }

    @Test
    void greaterThanEq_with_array_literal_throws_parseException() {
        assertThrows(
                SQL4JsonParseException.class,
                () -> QueryParser.parse("SELECT * FROM $r WHERE tags >= ARRAY['a']", DEFAULTS));
    }

    @Test
    void lessThanEq_with_array_literal_throws_parseException() {
        assertThrows(
                SQL4JsonParseException.class,
                () -> QueryParser.parse("SELECT * FROM $r WHERE tags <= ARRAY['a']", DEFAULTS));
    }

    @Test
    void greaterThan_with_array_literal_throws_parseException() {
        assertThrows(
                SQL4JsonParseException.class,
                () -> QueryParser.parse("SELECT * FROM $r WHERE tags > ARRAY['a']", DEFAULTS));
    }

    private static ConditionContext findFirstConditionContext(QueryDefinition def) {
        return findFirst(def.whereClause());
    }

    private static ConditionContext findFirst(CriteriaNode node) {
        if (node instanceof SingleConditionNode scn) return scn.cc();
        if (node instanceof AndNode an) return findFirst(an.left());
        if (node instanceof OrNode on) return findFirst(on.left());
        throw new IllegalStateException("no leaf");
    }
}
