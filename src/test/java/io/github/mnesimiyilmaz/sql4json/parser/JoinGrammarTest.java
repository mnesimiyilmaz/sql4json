package io.github.mnesimiyilmaz.sql4json.parser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JoinGrammarTest {

    @Test
    void parse_inner_join() {
        var qd = QueryParser.parse(
                "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id");
        assertEquals("users", qd.rootPath());
        assertEquals("u", qd.rootAlias());
        assertNotNull(qd.joins());
        assertEquals(1, qd.joins().size());

        var join = qd.joins().getFirst();
        assertEquals("orders", join.tableName());
        assertEquals("o", join.alias());
        assertEquals(JoinType.INNER, join.joinType());
        assertEquals(1, join.onConditions().size());
        assertEquals("u.id", join.onConditions().getFirst().leftPath());
        assertEquals("o.user_id", join.onConditions().getFirst().rightPath());
    }

    @Test
    void parse_explicit_inner_join() {
        var qd = QueryParser.parse(
                "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id");
        assertEquals(JoinType.INNER, qd.joins().getFirst().joinType());
    }

    @Test
    void parse_left_join() {
        var qd = QueryParser.parse(
                "SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id");
        assertEquals(JoinType.LEFT, qd.joins().getFirst().joinType());
    }

    @Test
    void parse_right_join() {
        var qd = QueryParser.parse(
                "SELECT o.amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id");
        assertEquals(JoinType.RIGHT, qd.joins().getFirst().joinType());
    }

    @Test
    void parse_chained_joins() {
        var qd = QueryParser.parse("""
                SELECT u.name, o.amount, p.name
                FROM users u
                JOIN orders o ON u.id = o.user_id
                LEFT JOIN products p ON o.product_id = p.id""");
        assertEquals(2, qd.joins().size());
        assertEquals("orders", qd.joins().get(0).tableName());
        assertEquals(JoinType.INNER, qd.joins().get(0).joinType());
        assertEquals("products", qd.joins().get(1).tableName());
        assertEquals(JoinType.LEFT, qd.joins().get(1).joinType());
    }

    @Test
    void parse_multi_column_on() {
        var qd = QueryParser.parse(
                "SELECT * FROM a a1 JOIN b b1 ON a1.x = b1.x AND a1.y = b1.y");
        var conditions = qd.joins().getFirst().onConditions();
        assertEquals(2, conditions.size());
        assertEquals("a1.x", conditions.get(0).leftPath());
        assertEquals("b1.x", conditions.get(0).rightPath());
        assertEquals("a1.y", conditions.get(1).leftPath());
        assertEquals("b1.y", conditions.get(1).rightPath());
    }

    @Test
    void parse_table_name_without_alias() {
        var qd = QueryParser.parse(
                "SELECT * FROM users JOIN orders ON users.id = orders.user_id");
        assertEquals("users", qd.rootPath());
        assertEquals("users", qd.rootAlias()); // defaults to table name
        assertEquals("orders", qd.joins().getFirst().alias()); // defaults to table name
    }

    @Test
    void parse_join_with_where_and_clauses() {
        var qd = QueryParser.parse("""
                SELECT u.dept, COUNT(*) AS cnt
                FROM users u
                JOIN orders o ON u.id = o.user_id
                WHERE o.status = 'completed'
                GROUP BY u.dept
                HAVING cnt >= 5
                ORDER BY cnt DESC
                LIMIT 10""");
        assertEquals("users", qd.rootPath());
        assertEquals(1, qd.joins().size());
        assertNotNull(qd.whereClause());
        assertNotNull(qd.groupBy());
        assertNotNull(qd.havingClause());
        assertNotNull(qd.orderBy());
        assertEquals(10, qd.limit());
    }

    @Test
    void parse_non_equality_on_operator_throws() {
        assertThrows(Exception.class, () -> QueryParser.parse(
                "SELECT * FROM a a1 JOIN b b1 ON a1.id > b1.id"));
    }

    @Test
    void existing_root_path_queries_still_work() {
        var qd = QueryParser.parse("SELECT * FROM $r WHERE age > 25");
        assertEquals("$r", qd.rootPath());
        assertNull(qd.rootAlias());
        assertNull(qd.joins());
    }

    @Test
    void existing_nested_path_queries_still_work() {
        var qd = QueryParser.parse("SELECT * FROM $r.data.items");
        assertEquals("$r.data.items", qd.rootPath());
        assertNull(qd.rootAlias());
        assertNull(qd.joins());
    }

    @Test
    void existing_subquery_still_works() {
        var qd = QueryParser.parse("SELECT * FROM (SELECT name FROM $r)");
        assertNotNull(qd.fromSubQuery());
        assertNull(qd.joins());
    }
}
