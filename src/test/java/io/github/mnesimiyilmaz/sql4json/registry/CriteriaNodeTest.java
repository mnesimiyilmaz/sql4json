package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Row;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CriteriaNodeTest {

    // An empty eager Row is sufficient for tests that use constant predicates
    private static final Row ANY_ROW = Row.eager(Map.of());

    // ── CriteriaNode as @FunctionalInterface ─────────────────────────────────

    @Test
    void criteriaNode_lambdaUsage() {
        CriteriaNode always = row -> true;
        assertTrue(always.test(ANY_ROW));
    }

    @Test
    void criteriaNode_lambdaFalse() {
        CriteriaNode never = row -> false;
        assertFalse(never.test(ANY_ROW));
    }

    // ── AndNode ──────────────────────────────────────────────────────────────

    @Test
    void andNode_bothTrue_returnsTrue() {
        AndNode and = new AndNode(row -> true, row -> true);
        assertTrue(and.test(ANY_ROW));
    }

    @Test
    void andNode_leftFalse_returnsFalse() {
        AndNode and = new AndNode(row -> false, row -> true);
        assertFalse(and.test(ANY_ROW));
    }

    @Test
    void andNode_rightFalse_returnsFalse() {
        AndNode and = new AndNode(row -> true, row -> false);
        assertFalse(and.test(ANY_ROW));
    }

    @Test
    void andNode_bothFalse_returnsFalse() {
        AndNode and = new AndNode(row -> false, row -> false);
        assertFalse(and.test(ANY_ROW));
    }

    @Test
    void andNode_shortCircuit_rightNotEvaluatedIfLeftFalse() {
        AndNode and = new AndNode(row -> false, row -> {
            throw new RuntimeException("right side must NOT be evaluated");
        });
        assertDoesNotThrow(() -> and.test(ANY_ROW));
    }

    // ── OrNode ───────────────────────────────────────────────────────────────

    @Test
    void orNode_bothTrue_returnsTrue() {
        OrNode or = new OrNode(row -> true, row -> true);
        assertTrue(or.test(ANY_ROW));
    }

    @Test
    void orNode_leftTrue_returnsTrue() {
        OrNode or = new OrNode(row -> true, row -> false);
        assertTrue(or.test(ANY_ROW));
    }

    @Test
    void orNode_rightTrue_returnsTrue() {
        OrNode or = new OrNode(row -> false, row -> true);
        assertTrue(or.test(ANY_ROW));
    }

    @Test
    void orNode_bothFalse_returnsFalse() {
        OrNode or = new OrNode(row -> false, row -> false);
        assertFalse(or.test(ANY_ROW));
    }

    @Test
    void orNode_shortCircuit_rightNotEvaluatedIfLeftTrue() {
        OrNode or = new OrNode(row -> true, row -> {
            throw new RuntimeException("right side must NOT be evaluated");
        });
        assertDoesNotThrow(() -> or.test(ANY_ROW));
    }

    // ── Nesting ──────────────────────────────────────────────────────────────

    @Test
    void nested_andInsideOr() {
        // (false AND true) OR true → true
        OrNode or = new OrNode(
                new AndNode(row -> false, row -> true),
                row -> true);
        assertTrue(or.test(ANY_ROW));
    }

    @Test
    void nested_orInsideAnd() {
        // (true OR false) AND (false OR true) → true
        AndNode and = new AndNode(
                new OrNode(row -> true, row -> false),
                new OrNode(row -> false, row -> true));
        assertTrue(and.test(ANY_ROW));
    }

    @Test
    void nested_allFalse() {
        // (false OR false) AND (true AND false) → false
        AndNode and = new AndNode(
                new OrNode(row -> false, row -> false),
                new AndNode(row -> true, row -> false));
        assertFalse(and.test(ANY_ROW));
    }

    // ── ConditionContext ─────────────────────────────────────────────────────

    @Test
    void conditionContext_columnName_returns_null_when_lhsExpression_is_null() {
        var ctx = new ConditionContext(
                ConditionContext.ConditionType.IS_NULL,
                null,         // lhsExpression intentionally null
                null, null, null, null, null, null,
                null, null, null);
        assertNull(ctx.columnName());
    }

    @Test
    void conditionContext_columnName_returns_innermost_path_when_lhsExpression_present() {
        var lhs = new io.github.mnesimiyilmaz.sql4json.engine.Expression.ColumnRef("foo.bar");
        var ctx = new ConditionContext(
                ConditionContext.ConditionType.IS_NULL,
                lhs,
                null, null, null, null, null, null,
                null, null, null);
        assertEquals("foo.bar", ctx.columnName());
    }
}
