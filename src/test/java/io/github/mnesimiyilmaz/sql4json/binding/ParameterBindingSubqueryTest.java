// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.binding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import org.junit.jupiter.api.Test;

class ParameterBindingSubqueryTest {

    @Test
    void when_outer_has_subquery_with_positional_params_then_outer_positional_count_is_global() {
        QueryDefinition def = QueryParser.parse(
                "SELECT * FROM (SELECT * FROM $r WHERE age > ?) WHERE salary > ?", Sql4jsonSettings.defaults());
        // Subquery has 1 `?`, outer has 1 `?` → total 2
        assertEquals(2, def.positionalCount());
    }

    @Test
    void when_outer_has_subquery_with_positional_param_then_outer_subqueryOffset_is_zero() {
        // Outer has 0 `?` before the subquery, subquery has 1 `?`, outer has 1 after.
        QueryDefinition def = QueryParser.parse(
                "SELECT * FROM (SELECT * FROM $r WHERE age > ?) WHERE salary > ?", Sql4jsonSettings.defaults());
        assertEquals(0, def.subqueryPositionalOffset(), "Outer saw 0 `?` before entering the subquery, so offset is 0");
        assertEquals(2, def.positionalCount());
    }

    @Test
    void when_outer_has_subquery_with_named_params_then_named_union() {
        QueryDefinition def = QueryParser.parse(
                "SELECT * FROM (SELECT * FROM $r WHERE dept = :d) WHERE salary > :min", Sql4jsonSettings.defaults());
        assertTrue(def.namedParameters().contains("d"));
        assertTrue(def.namedParameters().contains("min"));
        assertEquals(2, def.namedParameters().size());
    }
}
