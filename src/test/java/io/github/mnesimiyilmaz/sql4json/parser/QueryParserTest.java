package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.registry.AndNode;
import io.github.mnesimiyilmaz.sql4json.registry.OrNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class QueryParserTest {

    @Test
    void parse_select_star() {
        var qd = QueryParser.parse("SELECT * FROM $r");
        assertTrue(qd.isSelectAll());
        assertEquals("$r", qd.rootPath());
        assertNull(qd.whereClause());
        assertNull(qd.groupBy());
        assertNull(qd.orderBy());
    }

    @Test
    void parse_select_specific_columns() {
        var qd = QueryParser.parse("SELECT name, age FROM $r");
        assertEquals(2, qd.selectedColumns().size());
        assertEquals("name", qd.selectedColumns().get(0).columnName());
        assertEquals("age", qd.selectedColumns().get(1).columnName());
        assertFalse(qd.isSelectAll());
    }

    @Test
    void parse_column_with_alias() {
        var qd = QueryParser.parse("SELECT name AS n FROM $r");
        assertEquals(1, qd.selectedColumns().size());
        var col = qd.selectedColumns().get(0);
        assertEquals("name", col.columnName());
        assertEquals("n", col.alias());
        assertEquals("n", col.aliasOrName());
    }

    @ParameterizedTest(name = "parse_where_referencesField[{0} -> {1}]")
    @CsvSource({
            "SELECT * FROM $r WHERE age > 25, age",
            "SELECT * FROM $r WHERE name LIKE 'A%', name",
            "SELECT * FROM $r WHERE LOWER(name) = 'john', name"
    })
    void parse_where_referencesField(String sql, String expectedField) {
        var qd = QueryParser.parse(sql);
        assertNotNull(qd.whereClause());
        assertTrue(qd.referencedFields().contains(expectedField));
    }

    @Test
    void referencedColumns_returns_field_keys_for_each_referenced_field() {
        var qd = QueryParser.parse("SELECT name, age FROM $r WHERE dept = 'Eng' GROUP BY dept");
        Set<FieldKey> cols = qd.referencedColumns();
        // Mirror the underlying referencedFields set, projected to FieldKey
        assertEquals(qd.referencedFields().size(), cols.size());
        assertTrue(cols.contains(FieldKey.of("name")));
        assertTrue(cols.contains(FieldKey.of("age")));
        assertTrue(cols.contains(FieldKey.of("dept")));
    }

    @Test
    void referencedColumns_is_unmodifiable() {
        var qd = QueryParser.parse("SELECT name FROM $r");
        Set<FieldKey> cols = qd.referencedColumns();
        assertThrows(UnsupportedOperationException.class,
                () -> cols.add(FieldKey.of("extra")));
    }

    @Test
    void parse_where_and_or_precedence() {
        // AND binds tighter than OR: a=1 OR b=2 AND c=3 → a=1 OR (b=2 AND c=3)
        // Root should be OrNode: left=comparison(a=1), right=AndNode(b=2, c=3)
        var qd = QueryParser.parse("SELECT * FROM $r WHERE a = 1 OR b = 2 AND c = 3");
        assertInstanceOf(OrNode.class, qd.whereClause());
        OrNode or = (OrNode) qd.whereClause();
        assertInstanceOf(AndNode.class, or.right());
    }

    @Test
    void parse_where_parentheses_override_precedence() {
        // (a=1 OR b=2) AND c=3 — parens force OR first
        var qd = QueryParser.parse("SELECT * FROM $r WHERE (a = 1 OR b = 2) AND c = 3");
        assertInstanceOf(AndNode.class, qd.whereClause());
        AndNode and = (AndNode) qd.whereClause();
        assertInstanceOf(OrNode.class, and.left());
    }

    @ParameterizedTest(name = "parse_where_clauseNotNull[{0}]")
    @CsvSource({
            "SELECT * FROM $r WHERE email IS NULL",
            "SELECT * FROM $r WHERE email IS NOT NULL",
            "SELECT * FROM $r WHERE temp > -10",
            "SELECT * FROM $r WHERE name = 'it''s'"
    })
    void parse_where_clauseNotNull(String sql) {
        var qd = QueryParser.parse(sql);
        assertNotNull(qd.whereClause());
    }

    @Test
    void parse_group_by() {
        var qd = QueryParser.parse("SELECT dept, COUNT(name) AS cnt FROM $r GROUP BY dept");
        assertEquals(1, qd.groupBy().size());
        assertEquals("dept", qd.groupBy().get(0).innermostColumnPath());
        assertTrue(qd.requiresFullFlatten());
        assertEquals(2, qd.selectedColumns().size());
        var countCol = qd.selectedColumns().get(1);
        assertEquals("COUNT", countCol.aggFunction());
        assertEquals("name", countCol.columnName());
        assertEquals("cnt", countCol.alias());
    }

    @Test
    void parse_order_by_asc() {
        var qd = QueryParser.parse("SELECT * FROM $r ORDER BY name ASC");
        assertEquals(1, qd.orderBy().size());
        assertEquals("name", qd.orderBy().get(0).columnName());
        assertEquals("ASC", qd.orderBy().get(0).direction());
    }

    @Test
    void parse_order_by_desc_default_asc() {
        var qd = QueryParser.parse("SELECT * FROM $r ORDER BY age DESC, name");
        assertEquals(2, qd.orderBy().size());
        assertEquals("DESC", qd.orderBy().get(0).direction());
        assertEquals("ASC", qd.orderBy().get(1).direction());
    }

    @Test
    void parse_nested_path_root() {
        var qd = QueryParser.parse("SELECT * FROM $r.data.items");
        assertEquals("$r.data.items", qd.rootPath());
    }

    @Test
    void parse_subquery_in_from() {
        var qd = QueryParser.parse(
                "SELECT name FROM (SELECT * FROM $r WHERE age > 25) WHERE name LIKE 'A%'");
        assertNotNull(qd.fromSubQuery());
        // fromSubQuery preserves original whitespace from the char stream
        assertTrue(qd.fromSubQuery().contains("age > 25"));
        assertNotNull(qd.whereClause());
    }

    @Test
    void parse_having_clause() {
        var qd = QueryParser.parse("SELECT dept, COUNT(name) AS cnt FROM $r GROUP BY dept HAVING cnt > 1");
        assertNotNull(qd.havingClause());
        assertNotNull(qd.groupBy());
        assertEquals(1, qd.groupBy().size());
        assertEquals("dept", qd.groupBy().get(0).innermostColumnPath());
    }

    @Test
    void parse_invalid_sql_throws_parse_exception() {
        assertThrows(SQL4JsonParseException.class,
                () -> QueryParser.parse("NOT VALID SQL"));
    }

    @Test
    void parse_empty_input_throws_parse_exception() {
        assertThrows(SQL4JsonParseException.class,
                () -> QueryParser.parse(""));
    }
}
