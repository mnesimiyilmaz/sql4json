// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.registry.*;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link SQL4JsonParserListener} tags every {@link Expression.ParameterRef} with the correct
 * {@link ParameterPositionKind} based on the placeholder's syntactic position. Consumed by the bind-time parameter
 * validator.
 */
class ParameterPositionKindTest {

    private static final Sql4jsonSettings DEFAULTS = Sql4jsonSettings.defaults();

    @Test
    void parameter_in_array_literal_has_ARRAY_ELEMENT_kind() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags @> ARRAY[?, ?]", DEFAULTS);
        List<Expression.ParameterRef> refs = collectParameterRefs(def);
        assertEquals(2, refs.size());
        for (Expression.ParameterRef r : refs) {
            assertEquals(
                    ParameterPositionKind.ARRAY_ELEMENT,
                    r.positionKind(),
                    "expected ARRAY_ELEMENT, got " + r.positionKind());
        }
    }

    @Test
    void bare_parameter_rhs_has_BARE_ARRAY_RHS_kind() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags @> :tagList", DEFAULTS);
        List<Expression.ParameterRef> refs = collectParameterRefs(def);
        assertEquals(1, refs.size());
        assertEquals(ParameterPositionKind.BARE_ARRAY_RHS, refs.getFirst().positionKind());
    }

    @Test
    void scalar_position_parameter_has_REGULAR_SCALAR_kind() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE name = :n", DEFAULTS);
        List<Expression.ParameterRef> refs = collectParameterRefs(def);
        assertEquals(1, refs.size());
        assertEquals(ParameterPositionKind.REGULAR_SCALAR, refs.getFirst().positionKind());
    }

    @Test
    void in_list_parameter_has_IN_LIST_kind() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE id IN (?)", DEFAULTS);
        List<Expression.ParameterRef> refs = collectParameterRefs(def);
        assertEquals(1, refs.size());
        assertEquals(ParameterPositionKind.IN_LIST, refs.getFirst().positionKind());
    }

    @Test
    void contains_keyword_parameter_has_REGULAR_SCALAR_kind() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags CONTAINS :v", DEFAULTS);
        List<Expression.ParameterRef> refs = collectParameterRefs(def);
        assertEquals(1, refs.size());
        assertEquals(ParameterPositionKind.REGULAR_SCALAR, refs.getFirst().positionKind());
    }

    @Test
    void array_equality_literal_parameter_has_ARRAY_ELEMENT_kind() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE tags = ARRAY[?, ?]", DEFAULTS);
        List<Expression.ParameterRef> refs = collectParameterRefs(def);
        assertEquals(2, refs.size());
        for (Expression.ParameterRef r : refs) {
            assertEquals(ParameterPositionKind.ARRAY_ELEMENT, r.positionKind());
        }
    }

    private static List<Expression.ParameterRef> collectParameterRefs(QueryDefinition def) {
        List<Expression.ParameterRef> out = new ArrayList<>();
        if (def.whereClause() != null) {
            walkCriteria(def.whereClause(), out);
        }
        if (def.havingClause() != null) {
            walkCriteria(def.havingClause(), out);
        }
        return out;
    }

    private static void walkCriteria(CriteriaNode node, List<Expression.ParameterRef> out) {
        switch (node) {
            case AndNode(CriteriaNode left, CriteriaNode right) -> {
                walkCriteria(left, out);
                walkCriteria(right, out);
            }
            case OrNode(CriteriaNode left, CriteriaNode right) -> {
                walkCriteria(left, out);
                walkCriteria(right, out);
            }
            case SingleConditionNode(ConditionContext cc, CriteriaNode ignored) -> walkCondition(cc, out);
            default -> {
                // Other CriteriaNode subtypes are not exercised by these tests.
            }
        }
    }

    private static void walkCondition(ConditionContext cc, List<Expression.ParameterRef> out) {
        walkExpression(cc.lhsExpression(), out);
        walkExpression(cc.rhsExpression(), out);
        walkExpression(cc.lowerBoundExpr(), out);
        walkExpression(cc.upperBoundExpr(), out);
        if (cc.valueExpressions() != null) {
            for (Expression e : cc.valueExpressions()) {
                walkExpression(e, out);
            }
        }
    }

    private static void walkExpression(Expression expr, List<Expression.ParameterRef> out) {
        if (expr == null) return;
        if (expr instanceof Expression.ParameterRef pr) {
            out.add(pr);
            return;
        }
        if (expr instanceof Expression.ScalarFnCall(var name, var args)) {
            for (Expression arg : args) {
                walkExpression(arg, out);
            }
        }
        // Other expression types — ColumnRef, LiteralVal, NowRef, etc. — don't contain ParameterRef
        // in scenarios exercised here. CASE WHEN bodies are not covered.
    }
}
