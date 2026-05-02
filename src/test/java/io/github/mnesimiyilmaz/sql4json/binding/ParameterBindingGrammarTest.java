// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.binding;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.BoundParameters;
import io.github.mnesimiyilmaz.sql4json.SQL4Json;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import org.junit.jupiter.api.Test;

/**
 * Grammar-coverage tests: confirm placeholders work in every value context the design mentions, plus the error paths
 * (mode mixing, {@code maxParameters}, invalid names).
 *
 * <p>Distinct from {@code ParameterBindingIntegrationTest} which focuses on higher-level API scenarios — this class is
 * the parse-side coverage net.
 */
class ParameterBindingGrammarTest {

    private static final String JSON = """
            [{"n":1,"s":"Alice","d":"2026-01-01"},
             {"n":2,"s":"Bob","d":"2026-02-01"},
             {"n":3,"s":"Carol","d":"2026-03-01"}]
            """;

    @Test
    void when_placeholder_in_equality() {
        assertTrue(SQL4Json.prepare("SELECT s FROM $r WHERE n = :n")
                .execute(JSON, BoundParameters.named().bind("n", 2))
                .contains("Bob"));
    }

    @Test
    void when_placeholder_in_between() {
        String r = SQL4Json.prepare("SELECT s FROM $r WHERE n BETWEEN :lo AND :hi")
                .execute(JSON, BoundParameters.named().bind("lo", 2).bind("hi", 3));
        assertTrue(r.contains("Bob"));
        assertTrue(r.contains("Carol"));
        assertFalse(r.contains("Alice"));
    }

    @Test
    void when_placeholder_in_like() {
        String r = SQL4Json.prepare("SELECT s FROM $r WHERE s LIKE :pat")
                .execute(JSON, BoundParameters.named().bind("pat", "A%"));
        assertTrue(r.contains("Alice"));
        assertFalse(r.contains("Bob"));
    }

    @Test
    void when_placeholder_in_in_list_multi() {
        String r = SQL4Json.prepare("SELECT s FROM $r WHERE n IN (?, ?)").execute(JSON, 1, 3);
        assertTrue(r.contains("Alice"));
        assertTrue(r.contains("Carol"));
        assertFalse(r.contains("Bob"));
    }

    @Test
    void when_placeholder_in_function_arg() {
        String r = SQL4Json.prepare("SELECT s FROM $r WHERE LOWER(s) = :x")
                .execute(JSON, BoundParameters.named().bind("x", "alice"));
        assertTrue(r.contains("Alice"));
    }

    @Test
    void when_placeholder_in_nested_function_arg() {
        String r = SQL4Json.prepare("SELECT s FROM $r WHERE UPPER(LOWER(s)) = :x")
                .execute(JSON, BoundParameters.named().bind("x", "BOB"));
        assertTrue(r.contains("Bob"));
    }

    @Test
    void when_mixed_positional_and_named_then_parse_exception() {
        assertThrows(SQL4JsonParseException.class, () -> SQL4Json.prepare("SELECT * FROM $r WHERE n = ? AND s = :s"));
    }

    @Test
    void when_maxParameters_exceeded_then_parse_exception() {
        Sql4jsonSettings tight =
                Sql4jsonSettings.builder().limits(l -> l.maxParameters(2)).build();
        assertThrows(
                SQL4JsonParseException.class,
                () -> SQL4Json.prepare("SELECT * FROM $r WHERE n = ? AND n = ? AND n = ?", tight));
    }

    @Test
    void when_string_literal_contains_colon_name_like_text_then_not_treated_as_parameter() {
        // Literal string containing ":fake" — must parse as STRING, not a placeholder.
        String r = SQL4Json.prepare("SELECT s FROM $r WHERE s = ':fake' OR n = :n")
                .execute(JSON, BoundParameters.named().bind("n", 1));
        assertTrue(r.contains("Alice"));
    }

    @Test
    void when_invalid_named_identifier_starts_with_digit_then_parse_exception() {
        assertThrows(SQL4JsonParseException.class, () -> SQL4Json.prepare("SELECT * FROM $r WHERE n = :1bad"));
    }

    @Test
    void when_placeholder_in_select_literal_projection() {
        // Placeholder as a literal in SELECT list.
        String r = SQL4Json.prepare("SELECT :label AS tag, s FROM $r WHERE n = :n")
                .execute(JSON, BoundParameters.named().bind("label", "person").bind("n", 2));
        assertTrue(r.contains("person"));
        assertTrue(r.contains("Bob"));
    }

    @Test
    void when_placeholder_in_order_by_literal_side() {
        // Direction can't be parameterised (it's a keyword), but literals inside ORDER BY
        // expressions can. This one just confirms the query parses cleanly with a param
        // used later in WHERE + fixed ORDER BY direction.
        String r = SQL4Json.prepare("SELECT s FROM $r WHERE n >= :min ORDER BY n DESC")
                .execute(JSON, BoundParameters.named().bind("min", 2));
        // Both Bob (n=2) and Carol (n=3) qualify; ORDER BY DESC puts Carol first.
        int carolIdx = r.indexOf("Carol");
        int bobIdx = r.indexOf("Bob");
        assertTrue(carolIdx >= 0 && bobIdx >= 0);
        assertTrue(carolIdx < bobIdx);
    }
}
