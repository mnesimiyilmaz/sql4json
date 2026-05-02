// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SQL4JsonArrayPredicatesTest {

    @Test
    void contains_scalar_matches_when_element_present() {
        String json = """
                [
                  {"id": 1, "tags": ["admin", "editor"]},
                  {"id": 2, "tags": ["viewer"]},
                  {"id": 3, "tags": ["admin"]}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 'admin'", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertTrue(result.contains("\"id\":3"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }

    @Test
    void contains_scalar_returns_no_rows_when_field_missing() {
        String json = """
                [{"id": 1, "name": "x"}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 'admin'", json);
        assertEquals("[]", result);
    }

    @Test
    void contains_scalar_returns_no_rows_when_field_is_null() {
        String json = """
                [{"id": 1, "tags": null}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 'admin'", json);
        assertEquals("[]", result);
    }

    @Test
    void contains_scalar_returns_no_rows_when_field_is_scalar() {
        String json = """
                [{"id": 1, "tags": "admin"}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 'admin'", json);
        assertEquals("[]", result);
    }

    @Test
    void contains_with_null_rhs_always_false() {
        String json = """
                [{"id": 1, "tags": ["admin", null]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS NULL", json);
        assertEquals("[]", result);
    }

    @Test
    void contains_against_numeric_array() {
        String json = """
                [{"id": 1, "scores": [10, 20, 30]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE scores CONTAINS 20", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void contains_does_not_match_different_type() {
        String json = """
                [{"id": 1, "tags": ["1", "2"]}]
                """;
        // string '1' != number 1 per SqlValueComparator
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 1", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayContains_with_literal_matches_when_all_elements_present() {
        String json = """
                [
                  {"id": 1, "tags": ["admin", "editor", "viewer"]},
                  {"id": 2, "tags": ["admin"]},
                  {"id": 3, "tags": ["editor"]}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags @> ARRAY['admin','editor']", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
        assertFalse(result.contains("\"id\":3"), result);
    }

    @Test
    void arrayContains_with_empty_literal_always_true_for_array_field() {
        String json = """
                [{"id": 1, "tags": ["admin"]}, {"id": 2, "tags": []}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags @> ARRAY[]", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertTrue(result.contains("\"id\":2"), result);
    }

    @Test
    void arrayContainedBy_matches_when_left_is_subset() {
        String json = """
                [
                  {"id": 1, "tags": ["admin"]},
                  {"id": 2, "tags": ["admin","editor"]},
                  {"id": 3, "tags": ["admin","superuser"]}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags <@ ARRAY['admin','editor','viewer']", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertTrue(result.contains("\"id\":2"), result);
        assertFalse(result.contains("\"id\":3"), result);
    }

    @Test
    void arrayContainedBy_with_empty_left_always_true() {
        String json = """
                [{"id": 1, "tags": []}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags <@ ARRAY['admin']", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void arrayOverlap_matches_when_any_common() {
        String json = """
                [
                  {"id": 1, "tags": ["admin","editor"]},
                  {"id": 2, "tags": ["viewer"]},
                  {"id": 3, "tags": ["editor","other"]}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags && ARRAY['admin','superuser']", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
        assertFalse(result.contains("\"id\":3"), result);
    }

    @Test
    void arrayOverlap_with_empty_literal_always_false() {
        String json = """
                [{"id": 1, "tags": ["admin"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags && ARRAY[]", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayContains_returns_false_when_lhs_field_missing() {
        String json = """
                [{"id": 1, "name": "x"}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags @> ARRAY['admin']", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayContains_with_named_list_parameter() {
        String json = """
                [
                  {"id": 1, "tags": ["admin","editor"]},
                  {"id": 2, "tags": ["viewer"]}
                ]
                """;
        String result = SQL4Json.prepare("SELECT id FROM $r WHERE tags @> :req")
                .execute(json, BoundParameters.named().bind("req", java.util.List.of("admin", "editor")));
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }

    @Test
    void arrayOverlap_with_positional_list_parameter() {
        String json = """
                [
                  {"id": 1, "tags": ["admin","editor"]},
                  {"id": 2, "tags": ["viewer"]}
                ]
                """;
        String result = SQL4Json.prepare("SELECT id FROM $r WHERE tags && ?")
                .execute(json, BoundParameters.of(java.util.List.of("editor", "other")));
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }

    @Test
    void arrayContains_with_empty_list_parameter() {
        String json = """
                [{"id": 1, "tags": ["admin"]}]
                """;
        String result = SQL4Json.prepare("SELECT id FROM $r WHERE tags @> :req")
                .execute(json, BoundParameters.named().bind("req", java.util.List.<String>of()));
        // empty subset → always true
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void arrayContainedBy_with_named_list_parameter() {
        String json = """
                [
                  {"id": 1, "tags": ["a"]},
                  {"id": 2, "tags": ["a","b","c"]}
                ]
                """;
        String result = SQL4Json.prepare("SELECT id FROM $r WHERE tags <@ :allowed")
                .execute(json, BoundParameters.named().bind("allowed", java.util.List.of("a", "b")));
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }

    @Test
    void arrayEquals_matches_only_on_exact_match_in_order() {
        String json = """
                [
                  {"id": 1, "tags": ["admin","editor"]},
                  {"id": 2, "tags": ["editor","admin"]},
                  {"id": 3, "tags": ["admin"]}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags = ARRAY['admin','editor']", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
        assertFalse(result.contains("\"id\":3"), result);
    }

    @Test
    void arrayNotEquals_negates_array_equals() {
        String json = """
                [
                  {"id": 1, "tags": ["admin","editor"]},
                  {"id": 2, "tags": ["editor","admin"]}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags != ARRAY['admin','editor']", json);
        assertTrue(result.contains("\"id\":2"), result);
        assertFalse(result.contains("\"id\":1"), result);
    }

    @Test
    void arrayEquals_with_empty_literal_matches_empty_array() {
        String json = """
                [{"id": 1, "tags": []}, {"id": 2, "tags": ["a"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags = ARRAY[]", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }

    @Test
    void arrayEquals_returns_false_for_missing_field() {
        String json = """
                [{"id": 1, "name": "x"}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags = ARRAY['admin']", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayNotEquals_with_missing_field_returns_no_rows() {
        // navigateToArray returns null for missing field → predicate false → no row;
        // ARRAY_NOT_EQUALS keeps SQL-standard semantics (null-bearing → false).
        String json = """
                [{"id": 1, "name": "x"}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags != ARRAY['admin']", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayEquals_with_null_element_returns_false() {
        String json = """
                [{"id": 1, "tags": ["admin", null]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags = ARRAY['admin','editor']", json);
        assertEquals("[]", result);
    }

    @Test
    void contains_skips_null_elements_in_haystack() {
        String json = """
                [{"id": 1, "tags": ["admin", null, "editor"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 'admin'", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void mixed_type_array_compares_per_element_type() {
        String json = """
                [{"id": 1, "tags": ["admin", 1, true]}]
                """;
        String r1 = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 'admin'", json);
        String r2 = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 1", json);
        String r3 = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS '1'", json);
        assertTrue(r1.contains("\"id\":1"), r1);
        assertTrue(r2.contains("\"id\":1"), r2);
        assertEquals("[]", r3); // string '1' != number 1 per SqlValueComparator
    }

    @Test
    void duplicates_treated_as_set_for_arrayContains() {
        String json = """
                [{"id": 1, "tags": ["admin", "admin"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags @> ARRAY['admin']", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void duplicates_break_strict_array_equals() {
        String json = """
                [{"id": 1, "tags": ["admin", "admin"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags = ARRAY['admin']", json);
        assertEquals("[]", result);
    }

    @Test
    void contains_inside_nested_array_via_dot_path() {
        String json = """
                [
                  {"id": 1, "user": {"tags": ["admin","editor"]}},
                  {"id": 2, "user": {"tags": ["viewer"]}}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE user.tags CONTAINS 'admin'", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }

    @Test
    void arrayContains_with_boolean_elements() {
        String json = """
                [{"id": 1, "flags": [true, false]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE flags @> ARRAY[true]", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void array_predicate_composes_with_AND_OR() {
        String json = """
                [
                  {"id": 1, "active": true,  "tags": ["admin"]},
                  {"id": 2, "active": false, "tags": ["admin"]},
                  {"id": 3, "active": true,  "tags": ["viewer"]}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 'admin' AND active = true", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
        assertFalse(result.contains("\"id\":3"), result);
    }

    @Test
    void array_predicate_inside_searched_CASE_WHEN() {
        String json = """
                [
                  {"id": 1, "tags": ["admin"]},
                  {"id": 2, "tags": ["viewer"]}
                ]
                """;
        String result = SQL4Json.query(
                "SELECT id, CASE WHEN tags CONTAINS 'admin' THEN 'yes' ELSE 'no' END AS isAdmin FROM $r", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertTrue(result.contains("\"isAdmin\":\"yes\""), result);
        assertTrue(result.contains("\"id\":2"), result);
        assertTrue(result.contains("\"isAdmin\":\"no\""), result);
    }

    @Test
    void array_predicate_in_HAVING_clause() {
        String json = """
                [
                  {"dept": "eng", "tags": ["admin"]},
                  {"dept": "eng", "tags": ["viewer"]},
                  {"dept": "ops", "tags": ["admin"]}
                ]
                """;
        String result = SQL4Json.query(
                "SELECT dept, COUNT(*) AS c FROM $r WHERE tags CONTAINS 'admin' " + "GROUP BY dept HAVING c > 0", json);
        assertTrue(result.contains("\"dept\":\"eng\""), result);
        assertTrue(result.contains("\"dept\":\"ops\""), result);
    }

    @Test
    void array_predicate_in_parenthesized_OR_group() {
        String json = """
                [
                  {"id": 1, "tags": ["admin"]},
                  {"id": 2, "tags": ["editor"]},
                  {"id": 3, "tags": ["viewer"]}
                ]
                """;
        String result =
                SQL4Json.query("SELECT id FROM $r WHERE (tags CONTAINS 'admin' OR tags CONTAINS 'editor')", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertTrue(result.contains("\"id\":2"), result);
        assertFalse(result.contains("\"id\":3"), result);
    }

    @Test
    void array_predicate_combined_with_array_equals_via_AND() {
        String json = """
                [
                  {"id": 1, "tags": ["admin","editor"]},
                  {"id": 2, "tags": ["editor","admin"]},
                  {"id": 3, "tags": ["admin"]}
                ]
                """;
        // Strict order match AND must contain editor
        String result = SQL4Json.query(
                "SELECT id FROM $r WHERE tags = ARRAY['admin','editor'] AND tags @> ARRAY['editor']", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
        assertFalse(result.contains("\"id\":3"), result);
    }

    @Test
    void overlap_with_LIKE_compose() {
        String json = """
                [
                  {"name": "alice",   "tags": ["admin"]},
                  {"name": "bob",     "tags": ["viewer"]},
                  {"name": "albert",  "tags": ["editor"]}
                ]
                """;
        String result =
                SQL4Json.query("SELECT name FROM $r WHERE name LIKE 'al%' AND tags && ARRAY['admin','editor']", json);
        assertTrue(result.contains("\"name\":\"alice\""), result);
        assertTrue(result.contains("\"name\":\"albert\""), result);
        assertFalse(result.contains("\"name\":\"bob\""), result);
    }

    @Test
    void array_predicate_works_with_aliased_LHS_in_inner_JOIN() {
        String users = """
                [
                  {"id": 1, "name": "alice", "tags": ["admin","editor"]},
                  {"id": 2, "name": "bob",   "tags": ["viewer"]}
                ]
                """;
        String orders = """
                [{"userId": 1, "total": 100}, {"userId": 2, "total": 50}]
                """;
        String result = SQL4Json.query(
                "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.userId "
                        + "WHERE u.tags @> ARRAY['admin']",
                java.util.Map.of("users", users, "orders", orders));
        assertTrue(result.contains("\"u.name\":\"alice\""), result);
        assertTrue(result.contains("\"o.total\":100"), result);
        assertFalse(result.contains("\"u.name\":\"bob\""), result);
    }

    @Test
    void array_predicate_LHS_on_right_side_of_LEFT_JOIN() {
        String users = """
                [{"id": 1, "name": "alice"}]
                """;
        String roles = """
                [{"userId": 1, "tags": ["admin","editor"]}]
                """;
        String result = SQL4Json.query(
                "SELECT u.name FROM users u LEFT JOIN roles r ON u.id = r.userId " + "WHERE r.tags CONTAINS 'admin'",
                java.util.Map.of("users", users, "roles", roles));
        assertTrue(result.contains("\"u.name\":\"alice\""), result);
    }

    @Test
    void contains_with_aliased_LHS_in_JOIN() {
        String users = """
                [
                  {"id": 1, "name": "alice", "tags": ["admin"]},
                  {"id": 2, "name": "bob",   "tags": ["editor"]}
                ]
                """;
        String orders = """
                [{"userId": 1}, {"userId": 2}]
                """;
        String result = SQL4Json.query(
                "SELECT u.name FROM users u JOIN orders o ON u.id = o.userId " + "WHERE u.tags CONTAINS 'admin'",
                java.util.Map.of("users", users, "orders", orders));
        assertTrue(result.contains("\"u.name\":\"alice\""), result);
        assertFalse(result.contains("\"u.name\":\"bob\""), result);
    }

    @Test
    void overlap_with_aliased_LHS_in_JOIN() {
        String users = """
                [
                  {"id": 1, "name": "alice", "tags": ["admin","viewer"]},
                  {"id": 2, "name": "bob",   "tags": ["editor"]}
                ]
                """;
        String orders = """
                [{"userId": 1}, {"userId": 2}]
                """;
        String result = SQL4Json.query(
                "SELECT u.name FROM users u JOIN orders o ON u.id = o.userId "
                        + "WHERE u.tags && ARRAY['admin','superuser']",
                java.util.Map.of("users", users, "orders", orders));
        assertTrue(result.contains("\"u.name\":\"alice\""), result);
        assertFalse(result.contains("\"u.name\":\"bob\""), result);
    }

    @Test
    void arrayContains_with_column_ref_rhs_matches_per_row() {
        String json = """
                [
                  {"id": 1, "tags": ["a","b","c"], "required": ["a","b"]},
                  {"id": 2, "tags": ["a"],         "required": ["a","b"]}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags @> required", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }

    @Test
    void arrayContains_with_column_ref_rhs_returns_false_when_rhs_missing() {
        String json = """
                [{"id": 1, "tags": ["a"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags @> required", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayContains_with_column_ref_rhs_returns_false_when_rhs_scalar() {
        String json = """
                [{"id": 1, "tags": ["a"], "required": "a"}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags @> required", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayContainedBy_with_column_ref_rhs() {
        String json = """
                [
                  {"id": 1, "tags": ["a"],     "allowed": ["a","b"]},
                  {"id": 2, "tags": ["a","c"], "allowed": ["a","b"]}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags <@ allowed", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }

    @Test
    void arrayContains_with_null_in_literal_returns_false() {
        String json = """
                [{"id": 1, "tags": ["admin","editor"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags @> ARRAY['admin', NULL]", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayContainedBy_with_null_element_in_lhs_returns_false() {
        String json = """
                [{"id": 1, "tags": ["a", null]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags <@ ARRAY['a','b']", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayOverlap_skips_null_in_rhs_literal() {
        String json = """
                [{"id": 1, "tags": ["admin"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags && ARRAY['admin', NULL]", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void arrayEquals_with_null_in_rhs_literal_short_circuits_false() {
        String json = """
                [{"id": 1, "tags": ["a","b"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags = ARRAY['a', NULL]", json);
        assertEquals("[]", result);
    }

    @Test
    void arrayContains_with_null_element_in_lhs_array() {
        // CONTAINS skips null elements in haystack — should still match a non-null needle
        String json = """
                [{"id": 1, "tags": [null, "admin"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS 'admin'", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void arrayContainsAll_skips_null_in_lhs_haystack() {
        // tags @> ARRAY['admin'] with [null,"admin"] hits the .filter(v -> !v.isNull()) null arm
        String json = """
                [{"id": 1, "tags": [null, "admin"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags @> ARRAY['admin']", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void arrayContainedBy_skips_null_in_rhs_literal() {
        // tags <@ ARRAY['a', NULL] with [a] — RHS filter should skip NULL, still find 'a'
        String json = """
                [{"id": 1, "tags": ["a"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags <@ ARRAY['a', NULL]", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void arrayOverlap_with_null_element_in_lhs_array() {
        // tags && ARRAY['admin'] with [null,"admin"] — LHS filter skips null, finds 'admin'
        String json = """
                [{"id": 1, "tags": [null, "admin"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags && ARRAY['admin']", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void arrayOverlap_skips_null_when_null_is_first_in_rhs() {
        // Place NULL first in the RHS list so the r.isNull() continue branch fires
        // before any non-null match — this covers the if (r.isNull()) true arm of overlap().
        String json = """
                [{"id": 1, "tags": ["admin"]}]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags && ARRAY[NULL, 'admin']", json);
        assertTrue(result.contains("\"id\":1"), result);
    }

    @Test
    void contains_with_column_ref_rhs_resolves_per_row() {
        // CONTAINS with a column-ref RHS — exercises resolveScalarRhs() rhsExpression branch.
        String json = """
                [
                  {"id": 1, "tags": ["admin","editor"], "needle": "admin"},
                  {"id": 2, "tags": ["viewer"],        "needle": "admin"}
                ]
                """;
        String result = SQL4Json.query("SELECT id FROM $r WHERE tags CONTAINS needle", json);
        assertTrue(result.contains("\"id\":1"), result);
        assertFalse(result.contains("\"id\":2"), result);
    }
}
