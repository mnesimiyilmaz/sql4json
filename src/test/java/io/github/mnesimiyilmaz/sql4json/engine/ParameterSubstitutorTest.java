package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.BoundParameters;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ParameterSubstitutor} at the expression level.
 */
class ParameterSubstitutorTest {

    private static final Sql4jsonSettings S = Sql4jsonSettings.defaults();

    @Test
    void when_positional_parameter_ref_then_replaced_with_literal() {
        Expression.ParameterRef ref = new Expression.ParameterRef(null, 0);
        BoundParameters params = BoundParameters.of(42);
        Expression out = ParameterSubstitutor.substituteExpression(ref, params, S);
        assertInstanceOf(Expression.LiteralVal.class, out);
        Expression.LiteralVal lv = (Expression.LiteralVal) out;
        assertInstanceOf(SqlNumber.class, lv.value());
    }

    @Test
    void when_named_parameter_ref_then_replaced_with_literal() {
        Expression.ParameterRef ref = new Expression.ParameterRef("x", -1);
        BoundParameters params = BoundParameters.named().bind("x", "hello");
        Expression out = ParameterSubstitutor.substituteExpression(ref, params, S);
        assertInstanceOf(Expression.LiteralVal.class, out);
        assertEquals(new SqlString("hello"), ((Expression.LiteralVal) out).value());
    }

    @Test
    void when_function_contains_parameter_ref_then_nested_replaced() {
        Expression.ParameterRef ref = new Expression.ParameterRef("x", -1);
        Expression.ScalarFnCall fn = new Expression.ScalarFnCall("upper", List.of(ref));
        BoundParameters params = BoundParameters.named().bind("x", "ada");
        Expression out = ParameterSubstitutor.substituteExpression(fn, params, S);
        assertInstanceOf(Expression.ScalarFnCall.class, out);
        Expression.ScalarFnCall outFn = (Expression.ScalarFnCall) out;
        assertInstanceOf(Expression.LiteralVal.class, outFn.args().get(0));
        assertEquals(new SqlString("ada"), ((Expression.LiteralVal) outFn.args().get(0)).value());
    }

    @Test
    void when_aggregate_inner_contains_parameter_ref_then_replaced() {
        Expression.ParameterRef ref = new Expression.ParameterRef("x", -1);
        Expression.AggregateFnCall agg = new Expression.AggregateFnCall("AVG", ref);
        BoundParameters params = BoundParameters.named().bind("x", 10);
        Expression out = ParameterSubstitutor.substituteExpression(agg, params, S);
        assertInstanceOf(Expression.AggregateFnCall.class, out);
        assertInstanceOf(Expression.LiteralVal.class, ((Expression.AggregateFnCall) out).inner());
    }

    @Test
    void when_aggregate_count_star_passthrough() {
        // COUNT(*) has inner == null — must not NPE.
        Expression.AggregateFnCall agg = new Expression.AggregateFnCall("COUNT", null);
        Expression out = ParameterSubstitutor.substituteExpression(agg, BoundParameters.EMPTY, S);
        assertSame(agg, out);
    }

    @Test
    void when_column_ref_literal_now_then_unchanged() {
        Expression col = new Expression.ColumnRef("name");
        Expression lit = new Expression.LiteralVal(new SqlString("x"));
        Expression now = new Expression.NowRef();
        assertSame(col, ParameterSubstitutor.substituteExpression(col, BoundParameters.EMPTY, S));
        assertSame(lit, ParameterSubstitutor.substituteExpression(lit, BoundParameters.EMPTY, S));
        assertSame(now, ParameterSubstitutor.substituteExpression(now, BoundParameters.EMPTY, S));
    }

    @Test
    void when_positional_param_index_out_of_range_then_exception() {
        Expression.ParameterRef ref = new Expression.ParameterRef(null, 5);
        BoundParameters params = BoundParameters.of(1);
        assertThrows(SQL4JsonBindException.class,
                () -> ParameterSubstitutor.substituteExpression(ref, params, S));
    }

    @Test
    void when_named_param_missing_then_exception() {
        Expression.ParameterRef ref = new Expression.ParameterRef("missing", -1);
        BoundParameters params = BoundParameters.named().bind("other", 1);
        assertThrows(SQL4JsonBindException.class,
                () -> ParameterSubstitutor.substituteExpression(ref, params, S));
    }

    @Test
    void when_simple_case_contains_parameter_ref_then_replaced() {
        // CASE subject WHEN :a THEN 'match' ELSE 'nope' END
        Expression.ParameterRef ref = new Expression.ParameterRef("a", -1);
        var whenClause = new Expression.WhenClause.ValueWhen(
                ref, new Expression.LiteralVal(new SqlString("match")));
        var sc = new Expression.SimpleCaseWhen(
                new Expression.ColumnRef("col"),
                List.of(whenClause),
                new Expression.LiteralVal(new SqlString("nope")));
        BoundParameters params = BoundParameters.named().bind("a", "foo");
        Expression out = ParameterSubstitutor.substituteExpression(sc, params, S);
        assertInstanceOf(Expression.SimpleCaseWhen.class, out);
        var outSc = (Expression.SimpleCaseWhen) out;
        assertInstanceOf(Expression.LiteralVal.class, outSc.whenClauses().get(0).value());
    }

    @Test
    void when_top_level_substitute_without_params_returns_def_unchanged() {
        // Parameterless queries short-circuit to the original definition.
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT * FROM $r WHERE age > 25", S);
        var out = ParameterSubstitutor.substitute(def, BoundParameters.EMPTY, S);
        assertSame(def, out);
    }

    @Test
    void when_where_has_parameter_then_substituted_definition_is_parameterless() {
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT * FROM $r WHERE age > ?", S);
        assertEquals(1, def.positionalCount());
        BoundParameters params = BoundParameters.of(25);
        var out = ParameterSubstitutor.substitute(def, params, S);
        assertEquals(0, out.positionalCount());
        assertNull(out.limitParam());
        assertNull(out.offsetParam());
        // The new where clause is a freshly resolved SingleConditionNode.
        assertNotSame(def.whereClause(), out.whereClause());
    }

    @Test
    void when_where_has_named_parameter_then_resolved_and_tested_successfully() {
        // Also exercise the runtime path: after substitution, the resolved CriteriaNode
        // should be present (sanity check that re-resolution happened).
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT * FROM $r WHERE dept = :d", S);
        BoundParameters params = BoundParameters.named().bind("d", "Eng");
        var out = ParameterSubstitutor.substitute(def, params, S);
        assertEquals(0, out.positionalCount());
        assertNotNull(out.whereClause());
    }

    @Test
    void when_limit_and_offset_params_then_resolved_to_integers() {
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT * FROM $r LIMIT ? OFFSET ?", S);
        assertNotNull(def.limitParam());
        assertNotNull(def.offsetParam());
        BoundParameters params = BoundParameters.of(20, 100);
        var out = ParameterSubstitutor.substitute(def, params, S);
        assertEquals(Integer.valueOf(20), out.limit());
        assertEquals(Integer.valueOf(100), out.offset());
        assertNull(out.limitParam());
        assertNull(out.offsetParam());
    }

    @Test
    void when_limit_bigInteger_exceeds_long_then_exception() {
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT * FROM $r LIMIT ?", S);
        java.math.BigInteger huge =
                java.math.BigInteger.valueOf(Long.MAX_VALUE).add(java.math.BigInteger.TEN);
        SQL4JsonBindException ex = assertThrows(SQL4JsonBindException.class,
                () -> ParameterSubstitutor.substitute(def, BoundParameters.of(huge), S));
        assertTrue(ex.getMessage().contains("exceeds Integer.MAX_VALUE"));
    }

    @Test
    void when_limit_bigDecimal_exceeds_long_then_exception() {
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT * FROM $r LIMIT ?", S);
        java.math.BigDecimal huge =
                new java.math.BigDecimal(Long.MAX_VALUE).add(java.math.BigDecimal.TEN);
        SQL4JsonBindException ex = assertThrows(SQL4JsonBindException.class,
                () -> ParameterSubstitutor.substitute(def, BoundParameters.of(huge), S));
        assertTrue(ex.getMessage().contains("exceeds Integer.MAX_VALUE"));
    }

    @Test
    void when_collection_bound_to_scalar_placeholder_then_exception() {
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT * FROM $r WHERE age = :a", S);
        SQL4JsonBindException ex = assertThrows(SQL4JsonBindException.class,
                () -> ParameterSubstitutor.substitute(
                        def, BoundParameters.named().bind("a", List.of(1, 2)), S));
        assertTrue(ex.getMessage().contains("Cannot bind collection"));
    }

    @Test
    void when_array_bound_to_scalar_placeholder_then_exception() {
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT * FROM $r WHERE age = ?", S);
        SQL4JsonBindException ex = assertThrows(SQL4JsonBindException.class,
                () -> ParameterSubstitutor.substitute(
                        def, BoundParameters.of(new int[]{1, 2}), S));
        assertTrue(ex.getMessage().contains("Cannot bind collection"));
    }

    @Test
    void when_groupBy_present_with_parameter_then_substituted() {
        // GROUP BY column is a literal; the parameter is in WHERE — exercises the
        // GROUP BY non-null branch of substitute()
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT dept, COUNT(*) FROM $r WHERE age > ? GROUP BY dept", S);
        var out = ParameterSubstitutor.substitute(def, BoundParameters.of(25), S);
        assertNotNull(out.groupBy());
        assertEquals(1, out.groupBy().size());
    }

    @Test
    void when_orderBy_present_with_parameter_then_substituted() {
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT name FROM $r WHERE age > ? ORDER BY name ASC", S);
        var out = ParameterSubstitutor.substitute(def, BoundParameters.of(25), S);
        assertNotNull(out.orderBy());
        assertEquals(1, out.orderBy().size());
    }

    @Test
    void when_window_function_with_parameter_then_substituted() {
        // LAG with a parameterized offset arg — exercises substituteWindowFn + substituteWindowSpec
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT LAG(name, ?) OVER (PARTITION BY dept ORDER BY age) FROM $r", S);
        var out = ParameterSubstitutor.substitute(def, BoundParameters.of(1), S);
        assertEquals(1, out.selectedColumns().size());
    }

    @Test
    void when_between_lower_bound_is_now_then_expression_path() {
        // NOW() as a BETWEEN bound survives substitution as Expression (non-literal path)
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT * FROM $r WHERE created BETWEEN NOW() AND :u", S);
        var out = ParameterSubstitutor.substitute(
                def,
                BoundParameters.named().bind("u", java.time.LocalDateTime.now()),
                S);
        assertNotNull(out.whereClause());
    }

    @Test
    void when_simpleCase_no_else_then_no_substitution_for_else() {
        // Simple CASE without ELSE — exercises the elseExpr == null branch
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT CASE age WHEN ? THEN 'a' END FROM $r", S);
        var out = ParameterSubstitutor.substitute(def, BoundParameters.of(30), S);
        assertEquals(1, out.selectedColumns().size());
    }

    @Test
    void when_searchedCase_no_else_then_no_substitution_for_else() {
        var def = io.github.mnesimiyilmaz.sql4json.parser.QueryParser.parse(
                "SELECT CASE WHEN age > ? THEN 'a' END FROM $r", S);
        var out = ParameterSubstitutor.substitute(def, BoundParameters.of(30), S);
        assertEquals(1, out.selectedColumns().size());
    }
}
