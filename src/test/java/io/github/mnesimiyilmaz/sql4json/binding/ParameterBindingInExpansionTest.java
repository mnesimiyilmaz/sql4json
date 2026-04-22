package io.github.mnesimiyilmaz.sql4json.binding;

import io.github.mnesimiyilmaz.sql4json.BoundParameters;
import io.github.mnesimiyilmaz.sql4json.engine.ParameterSubstitutor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises {@link ParameterSubstitutor}'s IN-list expansion logic and the scalar-position
 * guard against collections. Uses the substitutor directly + {@link QueryParser}.
 */
class ParameterBindingInExpansionTest {

    private static final Sql4jsonSettings S = Sql4jsonSettings.defaults();

    @Test
    void when_IN_single_placeholder_with_list_then_expanded() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE id IN (?)", S);
        QueryDefinition out = ParameterSubstitutor.substitute(
                def, BoundParameters.of(List.of(1, 2, 3)), S);
        assertNotNull(out);
        assertEquals(0, out.positionalCount());
    }

    @Test
    void when_IN_single_placeholder_with_single_scalar_then_single_element_list() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE id IN (?)", S);
        QueryDefinition out = ParameterSubstitutor.substitute(
                def, BoundParameters.of(42), S);
        assertEquals(0, out.positionalCount());
    }

    @Test
    void when_IN_single_placeholder_with_empty_list_then_ok() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE id IN (?)", S);
        // Empty collection is valid — expansion yields empty valueList; InConditionHandler
        // naturally returns false for empty lists (zero rows match).
        QueryDefinition out = ParameterSubstitutor.substitute(
                def, BoundParameters.of(List.of()), S);
        assertEquals(0, out.positionalCount());
    }

    @Test
    void when_IN_multi_placeholder_classic_arity_works() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE id IN (?, ?)", S);
        QueryDefinition out = ParameterSubstitutor.substitute(
                def, BoundParameters.of(1, 3), S);
        assertEquals(0, out.positionalCount());
    }

    @Test
    void when_IN_expansion_exceeds_max_then_exception() {
        Sql4jsonSettings tight = Sql4jsonSettings.builder()
                .limits(l -> l.maxInListSize(3)).build();
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE id IN (?)", tight);
        SQL4JsonBindException e = assertThrows(SQL4JsonBindException.class,
                () -> ParameterSubstitutor.substitute(
                        def, BoundParameters.of(List.of(1, 2, 3, 4, 5)), tight));
        assertTrue(e.getMessage().toLowerCase().contains("in list"),
                "message should mention IN list, got: " + e.getMessage());
    }

    @Test
    void when_collection_bound_to_scalar_placeholder_then_exception() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE id = ?", S);
        SQL4JsonBindException e = assertThrows(SQL4JsonBindException.class,
                () -> ParameterSubstitutor.substitute(
                        def, BoundParameters.of(List.of(1, 2)), S));
        assertTrue(e.getMessage().toLowerCase().contains("collection"),
                "message should mention collection, got: " + e.getMessage());
    }

    @Test
    void when_array_bound_to_IN_then_expanded() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r WHERE id IN (?)", S);
        Integer[] iarr = {1, 2, 3};
        QueryDefinition out = ParameterSubstitutor.substitute(
                def, BoundParameters.of((Object) iarr), S);
        assertEquals(0, out.positionalCount());
    }

    @Test
    void when_IN_NOT_IN_mixed_with_non_parameterised_scalar_then_ok() {
        QueryDefinition def = QueryParser.parse(
                "SELECT * FROM $r WHERE id NOT IN (?)", S);
        QueryDefinition out = ParameterSubstitutor.substitute(
                def, BoundParameters.of(List.of(1, 2)), S);
        assertEquals(0, out.positionalCount());
    }
}
