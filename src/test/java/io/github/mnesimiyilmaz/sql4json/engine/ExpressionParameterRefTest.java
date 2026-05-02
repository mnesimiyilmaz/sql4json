// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ExpressionParameterRefTest {

    @Test
    void when_positional_parameter_ref_then_name_null_and_index_set() {
        Expression.ParameterRef p = new Expression.ParameterRef(null, 3);
        assertNull(p.name());
        assertEquals(3, p.index());
    }

    @Test
    void when_named_parameter_ref_then_name_set_and_index_minus_one() {
        Expression.ParameterRef p = new Expression.ParameterRef("dept", -1);
        assertEquals("dept", p.name());
        assertEquals(-1, p.index());
    }

    @Test
    void when_parameter_ref_walked_by_collect_referenced_fields_then_nothing_added() {
        Expression.ParameterRef p = new Expression.ParameterRef("x", -1);
        Set<String> fields = new HashSet<>();
        p.collectReferencedFields(fields);
        assertTrue(fields.isEmpty());
    }

    @Test
    void when_parameter_ref_then_no_aggregate_no_window() {
        Expression.ParameterRef p = new Expression.ParameterRef(null, 0);
        assertFalse(p.containsAggregate());
        assertFalse(p.containsWindow());
    }

    @Test
    void when_parameter_ref_then_innermost_column_path_returns_null() {
        Expression.ParameterRef p = new Expression.ParameterRef(null, 0);
        assertNull(p.innermostColumnPath());
    }
}
