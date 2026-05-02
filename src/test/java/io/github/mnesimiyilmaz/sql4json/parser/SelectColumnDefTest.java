// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.parser;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.*;
import io.github.mnesimiyilmaz.sql4json.engine.WindowSpec;
import io.github.mnesimiyilmaz.sql4json.registry.CriteriaNode;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SelectColumnDefTest {

    @Test
    void aggFunction_directAggregate() {
        var col = SelectColumnDef.aggregate("COUNT", "name", "cnt");
        assertEquals("COUNT", col.aggFunction());
    }

    @Test
    void aggFunction_aggregateNestedInScalar() {
        // ROUND(AVG(salary), 2) — aggregate nested inside scalar
        Expression expr = new ScalarFnCall(
                "round", List.of(new AggregateFnCall("AVG", new ColumnRef("salary")), new LiteralVal(SqlNumber.of(2))));
        var col = SelectColumnDef.of(expr, "avg_sal");
        assertEquals("AVG", col.aggFunction());
    }

    @Test
    void aggFunction_noAggregate_returnsNull() {
        var col = SelectColumnDef.column("name");
        assertNull(col.aggFunction());
    }

    @Test
    void aggFunction_scalarOnly_returnsNull() {
        Expression expr = new ScalarFnCall("upper", List.of(new ColumnRef("name")));
        var col = SelectColumnDef.of(expr, null);
        assertNull(col.aggFunction());
    }

    @Test
    void aggFunction_asterisk_returnsNull() {
        var col = SelectColumnDef.asterisk();
        assertNull(col.aggFunction());
    }

    @Test
    void scalar_factoryMethod() {
        var col = SelectColumnDef.scalar("lower", "name", "lname", List.of(new SqlString("en-US")));
        assertEquals("name", col.columnName());
        assertEquals("lname", col.alias());
        assertTrue(col.expression() instanceof ScalarFnCall);
    }

    @Test
    void scalar_factoryMethod_nullArgs() {
        var col = SelectColumnDef.scalar("trim", "name", null, null);
        assertEquals("name", col.columnName());
        assertNull(col.alias());
    }

    @Test
    void aliasOrName_noAlias_usesColumnName() {
        var col = SelectColumnDef.column("name");
        assertEquals("name", col.aliasOrName());
    }

    @Test
    void aliasOrName_withAlias_usesAlias() {
        var col = SelectColumnDef.column("name", "n");
        assertEquals("n", col.aliasOrName());
    }

    @Test
    void aliasOrName_noColumnPath_usesToString() {
        // LiteralVal has no column path → falls back to expression.toString()
        var col = SelectColumnDef.of(new LiteralVal(SqlNumber.of(42)), null);
        assertNotNull(col.aliasOrName());
    }

    @Test
    void containsAggregate_true() {
        var col = SelectColumnDef.aggregate("SUM", "salary", "total");
        assertTrue(col.containsAggregate());
    }

    @Test
    void containsAggregate_false() {
        var col = SelectColumnDef.column("name");
        assertFalse(col.containsAggregate());
    }

    @Test
    void columnName_asterisk_returnsNull() {
        var col = SelectColumnDef.asterisk();
        assertNull(col.columnName());
    }

    @Test
    void containsWindow_true_for_window_function() {
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        var col = SelectColumnDef.of(winExpr, "row_num");

        assertTrue(col.containsWindow());
    }

    @Test
    void containsWindow_false_for_plain_column() {
        var col = SelectColumnDef.column("name");

        assertFalse(col.containsWindow());
    }

    @Test
    void containsWindow_false_for_aggregate() {
        var col = SelectColumnDef.aggregate("COUNT", "name", "cnt");

        assertFalse(col.containsWindow());
    }

    // ── findAggregate through CASE expressions ─────────────────────────────

    @Test
    void aggFunction_simpleCaseWhen_inSubject() {
        // findAggregate: SimpleCaseWhen → subject has aggregate
        Expression subject = new AggregateFnCall("COUNT", null);
        var clauses = List.of(
                new WhenClause.ValueWhen(new LiteralVal(SqlNumber.of(1)), new LiteralVal(new SqlString("one"))));
        Expression expr = new SimpleCaseWhen(subject, clauses, null);
        var col = SelectColumnDef.of(expr, "x");
        assertEquals("COUNT", col.aggFunction());
    }

    @Test
    void aggFunction_simpleCaseWhen_inClauseValue() {
        // findAggregate: SimpleCaseWhen → clause value has aggregate
        Expression subject = new ColumnRef("dept");
        var clauses = List.of(new WhenClause.ValueWhen(
                new AggregateFnCall("SUM", new ColumnRef("salary")), new LiteralVal(new SqlString("match"))));
        Expression expr = new SimpleCaseWhen(subject, clauses, null);
        var col = SelectColumnDef.of(expr, "x");
        assertEquals("SUM", col.aggFunction());
    }

    @Test
    void aggFunction_simpleCaseWhen_inClauseResult() {
        // findAggregate: SimpleCaseWhen → clause result has aggregate
        Expression subject = new ColumnRef("dept");
        var clauses = List.of(new WhenClause.ValueWhen(
                new LiteralVal(new SqlString("eng")), new AggregateFnCall("AVG", new ColumnRef("salary"))));
        Expression expr = new SimpleCaseWhen(subject, clauses, null);
        var col = SelectColumnDef.of(expr, "x");
        assertEquals("AVG", col.aggFunction());
    }

    @Test
    void aggFunction_simpleCaseWhen_inElse() {
        // findAggregate: SimpleCaseWhen → else has aggregate
        Expression subject = new ColumnRef("dept");
        var clauses = List.of(
                new WhenClause.ValueWhen(new LiteralVal(new SqlString("eng")), new LiteralVal(SqlNumber.of(0))));
        Expression elseExpr = new AggregateFnCall("MAX", new ColumnRef("salary"));
        Expression expr = new SimpleCaseWhen(subject, clauses, elseExpr);
        var col = SelectColumnDef.of(expr, "x");
        assertEquals("MAX", col.aggFunction());
    }

    @Test
    void aggFunction_simpleCaseWhen_noAggregate() {
        // findAggregate: SimpleCaseWhen → no aggregate anywhere → null
        Expression subject = new ColumnRef("dept");
        var clauses = List.of(new WhenClause.ValueWhen(
                new LiteralVal(new SqlString("eng")), new LiteralVal(new SqlString("Engineering"))));
        Expression expr = new SimpleCaseWhen(subject, clauses, new LiteralVal(new SqlString("Other")));
        var col = SelectColumnDef.of(expr, "x");
        assertNull(col.aggFunction());
    }

    @Test
    void aggFunction_simpleCaseWhen_nullElse() {
        // findAggregate: SimpleCaseWhen → elseExpr is null → null
        Expression subject = new ColumnRef("dept");
        var clauses = List.of(new WhenClause.ValueWhen(
                new LiteralVal(new SqlString("eng")), new LiteralVal(new SqlString("Engineering"))));
        Expression expr = new SimpleCaseWhen(subject, clauses, null);
        var col = SelectColumnDef.of(expr, "x");
        assertNull(col.aggFunction());
    }

    @Test
    void aggFunction_searchedCaseWhen_inResult() {
        // findAggregate: SearchedCaseWhen → clause result has aggregate
        CriteriaNode dummyCondition = row -> true;
        var clauses = List.of(new WhenClause.SearchWhen(
                dummyCondition, Set.of(), new AggregateFnCall("SUM", new ColumnRef("salary"))));
        Expression expr = new SearchedCaseWhen(clauses, null);
        var col = SelectColumnDef.of(expr, "x");
        assertEquals("SUM", col.aggFunction());
    }

    @Test
    void aggFunction_searchedCaseWhen_inElse() {
        // findAggregate: SearchedCaseWhen → else has aggregate
        CriteriaNode dummyCondition = row -> true;
        var clauses = List.of(new WhenClause.SearchWhen(dummyCondition, Set.of(), new LiteralVal(SqlNumber.of(0))));
        Expression elseExpr = new AggregateFnCall("MIN", new ColumnRef("salary"));
        Expression expr = new SearchedCaseWhen(clauses, elseExpr);
        var col = SelectColumnDef.of(expr, "x");
        assertEquals("MIN", col.aggFunction());
    }

    @Test
    void aggFunction_searchedCaseWhen_noAggregate() {
        // findAggregate: SearchedCaseWhen → no aggregate → null
        CriteriaNode dummyCondition = row -> true;
        var clauses =
                List.of(new WhenClause.SearchWhen(dummyCondition, Set.of(), new LiteralVal(new SqlString("match"))));
        Expression expr = new SearchedCaseWhen(clauses, new LiteralVal(new SqlString("no match")));
        var col = SelectColumnDef.of(expr, "x");
        assertNull(col.aggFunction());
    }

    @Test
    void aggFunction_searchedCaseWhen_nullElse() {
        // findAggregate: SearchedCaseWhen → elseExpr null, no aggregate in clause → null
        CriteriaNode dummyCondition = row -> true;
        var clauses =
                List.of(new WhenClause.SearchWhen(dummyCondition, Set.of(), new LiteralVal(new SqlString("match"))));
        Expression expr = new SearchedCaseWhen(clauses, null);
        var col = SelectColumnDef.of(expr, "x");
        assertNull(col.aggFunction());
    }

    @Test
    void aggFunction_windowFnCall_returnsNull() {
        // findAggregate: WindowFnCall → null (not aggregate)
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        var col = SelectColumnDef.of(winExpr, "rn");
        assertNull(col.aggFunction());
    }

    @Test
    void aggFunction_nowRef_returnsNull() {
        // findAggregate: NowRef → null
        var col = SelectColumnDef.of(new NowRef(), "ts");
        assertNull(col.aggFunction());
    }

    @Test
    void aggFunction_literalVal_returnsNull() {
        // findAggregate: LiteralVal → null
        var col = SelectColumnDef.of(new LiteralVal(SqlNumber.of(42)), "lit");
        assertNull(col.aggFunction());
    }

    // ── containsAggregate / containsWindow through CASE branches ───────────

    @Test
    void containsAggregate_simpleCaseWhen_true() {
        Expression subject = new AggregateFnCall("COUNT", null);
        var clauses = List.of(
                new WhenClause.ValueWhen(new LiteralVal(SqlNumber.of(1)), new LiteralVal(new SqlString("one"))));
        var col = SelectColumnDef.of(new SimpleCaseWhen(subject, clauses, null), null);
        assertTrue(col.containsAggregate());
    }

    @Test
    void containsAggregate_simpleCaseWhen_inClause() {
        Expression subject = new ColumnRef("x");
        var clauses = List.of(new WhenClause.ValueWhen(
                new LiteralVal(new SqlString("a")), new AggregateFnCall("SUM", new ColumnRef("y"))));
        var col = SelectColumnDef.of(new SimpleCaseWhen(subject, clauses, null), null);
        assertTrue(col.containsAggregate());
    }

    @Test
    void containsAggregate_simpleCaseWhen_inElse() {
        Expression subject = new ColumnRef("x");
        var clauses =
                List.of(new WhenClause.ValueWhen(new LiteralVal(new SqlString("a")), new LiteralVal(SqlNumber.of(0))));
        var col = SelectColumnDef.of(
                new SimpleCaseWhen(subject, clauses, new AggregateFnCall("MAX", new ColumnRef("y"))), null);
        assertTrue(col.containsAggregate());
    }

    @Test
    void containsAggregate_searchedCaseWhen_inResult() {
        CriteriaNode dummyCondition = row -> true;
        var clauses = List.of(
                new WhenClause.SearchWhen(dummyCondition, Set.of(), new AggregateFnCall("SUM", new ColumnRef("y"))));
        var col = SelectColumnDef.of(new SearchedCaseWhen(clauses, null), null);
        assertTrue(col.containsAggregate());
    }

    @Test
    void containsAggregate_searchedCaseWhen_inElse() {
        CriteriaNode dummyCondition = row -> true;
        var clauses = List.of(new WhenClause.SearchWhen(dummyCondition, Set.of(), new LiteralVal(SqlNumber.of(0))));
        var col =
                SelectColumnDef.of(new SearchedCaseWhen(clauses, new AggregateFnCall("MIN", new ColumnRef("y"))), null);
        assertTrue(col.containsAggregate());
    }

    @Test
    void containsWindow_simpleCaseWhen_subject() {
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        var clauses = List.of(
                new WhenClause.ValueWhen(new LiteralVal(SqlNumber.of(1)), new LiteralVal(new SqlString("one"))));
        var col = SelectColumnDef.of(new SimpleCaseWhen(winExpr, clauses, null), null);
        assertTrue(col.containsWindow());
    }

    @Test
    void containsWindow_simpleCaseWhen_clauseValue() {
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        var clauses = List.of(new WhenClause.ValueWhen(winExpr, new LiteralVal(new SqlString("one"))));
        var col = SelectColumnDef.of(new SimpleCaseWhen(new ColumnRef("x"), clauses, null), null);
        assertTrue(col.containsWindow());
    }

    @Test
    void containsWindow_simpleCaseWhen_clauseResult() {
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        var clauses = List.of(new WhenClause.ValueWhen(new LiteralVal(SqlNumber.of(1)), winExpr));
        var col = SelectColumnDef.of(new SimpleCaseWhen(new ColumnRef("x"), clauses, null), null);
        assertTrue(col.containsWindow());
    }

    @Test
    void containsWindow_simpleCaseWhen_else() {
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        var clauses = List.of(
                new WhenClause.ValueWhen(new LiteralVal(SqlNumber.of(1)), new LiteralVal(new SqlString("one"))));
        var col = SelectColumnDef.of(new SimpleCaseWhen(new ColumnRef("x"), clauses, winExpr), null);
        assertTrue(col.containsWindow());
    }

    @Test
    void containsWindow_searchedCaseWhen_result() {
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        CriteriaNode dummyCondition = row -> true;
        var clauses = List.of(new WhenClause.SearchWhen(dummyCondition, Set.of(), winExpr));
        var col = SelectColumnDef.of(new SearchedCaseWhen(clauses, null), null);
        assertTrue(col.containsWindow());
    }

    @Test
    void containsWindow_searchedCaseWhen_else() {
        var winExpr = new WindowFnCall("ROW_NUMBER", List.of(), new WindowSpec(List.of(), List.of()));
        CriteriaNode dummyCondition = row -> true;
        var clauses = List.of(new WhenClause.SearchWhen(dummyCondition, Set.of(), new LiteralVal(SqlNumber.of(0))));
        var col = SelectColumnDef.of(new SearchedCaseWhen(clauses, winExpr), null);
        assertTrue(col.containsWindow());
    }

    // ── containsAggregate/containsWindow asterisk ───────────────────────

    @Test
    void containsAggregate_asterisk() {
        var col = SelectColumnDef.asterisk();
        assertFalse(col.containsAggregate());
    }

    @Test
    void containsWindow_asterisk() {
        var col = SelectColumnDef.asterisk();
        assertFalse(col.containsWindow());
    }
}
