package io.github.mnesimiyilmaz.sql4json.grammar;

import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonLexer;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Enforces that {@link SQL4JsonGrammar}'s static tables stay in sync with
 * the generated lexer vocabulary and the {@link FunctionRegistry}.
 *
 * <p>When this test fails, update the static catalog in {@link SQL4JsonGrammar}
 * to match the newly-added or removed grammar/registry entry. The drift test
 * exists precisely to prevent silent divergence.
 */
class SQL4JsonGrammarDriftTest {

    @Test
    void keywordsMatchLexerVocabularyLiterals() {
        Set<String> derivedFromGrammar = deriveKeywordLiteralsFromLexer();
        Set<String> catalog = new HashSet<>(SQL4JsonGrammar.keywords());

        Set<String> missingFromCatalog = new HashSet<>(derivedFromGrammar);
        missingFromCatalog.removeAll(catalog);
        assertTrue(missingFromCatalog.isEmpty(),
                "Grammar has keywords not in SQL4JsonGrammar.keywords(): " + missingFromCatalog);

        Set<String> extraInCatalog = new HashSet<>(catalog);
        extraInCatalog.removeAll(derivedFromGrammar);
        assertTrue(extraInCatalog.isEmpty(),
                "SQL4JsonGrammar.keywords() has entries absent from the grammar: " + extraInCatalog);
    }

    @Test
    void scalarRegistryEntriesMatchCatalogScalarCategories() {
        Set<String> catalogScalar = SQL4JsonGrammar.functions().stream()
                .filter(f -> f.category() == Category.STRING
                        || f.category() == Category.MATH
                        || f.category() == Category.DATE_TIME
                        || f.category() == Category.CONVERSION)
                .map(FunctionInfo::name)
                .collect(Collectors.toSet());

        // The registry lumps NOW under valueFunctions; everything else under scalarFunctions.
        Set<String> registryScalarPlusValue = new HashSet<>();
        registryScalarPlusValue.addAll(FunctionRegistry.getDefault().scalarFunctionNames());
        registryScalarPlusValue.addAll(FunctionRegistry.getDefault().valueFunctionNames());

        assertEquals(registryScalarPlusValue, catalogScalar,
                "Scalar+Value registry differs from the catalog's scalar categories (STRING/MATH/DATE_TIME/CONVERSION)");
    }

    @Test
    void aggregateRegistryEntriesMatchCatalogAggregateCategory() {
        Set<String> catalogAggregate = SQL4JsonGrammar.functions().stream()
                .filter(f -> f.category() == Category.AGGREGATE)
                .map(FunctionInfo::name)
                .collect(Collectors.toSet());
        Set<String> registry = new HashSet<>(FunctionRegistry.getDefault().aggregateFunctionNames());
        assertEquals(registry, catalogAggregate);
    }

    @Test
    void windowFunctionCatalogMatchesGrammarList() {
        // Window functions are grammar-literal (see windowFunctionName rule);
        // any change to the grammar's window-function set must also update the catalog.
        Set<String> expected = Set.of("row_number", "rank", "dense_rank", "ntile", "lag", "lead");
        Set<String> actual = SQL4JsonGrammar.functions().stream()
                .filter(f -> f.category() == Category.WINDOW)
                .map(FunctionInfo::name)
                .collect(Collectors.toSet());
        assertEquals(expected, actual);
    }

    @Test
    void keywordsCatalog_includes_array_and_contains() {
        var keywords = SQL4JsonGrammar.keywords();
        assertTrue(keywords.contains("ARRAY"), "Expected ARRAY in keywords catalog");
        assertTrue(keywords.contains("CONTAINS"), "Expected CONTAINS in keywords catalog");
    }

    @Test
    void tokenize_covers_array_operators_with_no_BAD_TOKEN() {
        String sql = "tags @> ARRAY[1] && ARRAY[2] AND tags <@ ARRAY[3]";
        var tokens = SQL4JsonGrammar.tokenize(sql);
        var badTokens = tokens.stream()
                .filter(t -> t.kind() == TokenKind.BAD_TOKEN)
                .toList();
        assertTrue(badTokens.isEmpty(),
                "Expected no BAD_TOKEN entries; got: " + badTokens);
    }

    @Test
    void tokenKindMapCoversEveryLexerType() {
        var vocab = SQL4JsonLexer.VOCABULARY;
        Map<Integer, TokenKind> map = SQL4JsonGrammar.tokenKindByTypeForTesting();
        Set<String> uncovered = new TreeSet<>();
        for (int t = 1; t <= vocab.getMaxTokenType(); t++) {
            // Symbolic name is null for fragments and synthetic slots; those never
            // reach tokenize() so they need no TokenKind mapping.
            String symbolic = vocab.getSymbolicName(t);
            if (symbolic == null) continue;
            if (!map.containsKey(t)) {
                uncovered.add(symbolic + " (type " + t + ")");
            }
        }
        assertTrue(uncovered.isEmpty(),
                "TOKEN_KIND_BY_TYPE missing entries for grammar token types: " + uncovered
                        + ". Add the new lexer type(s) to SQL4JsonGrammar.buildTokenKindMap.");
    }

    /**
     * Reads {@code SQL4JsonLexer.VOCABULARY} and extracts the subset of literal
     * names that look like reserved keywords — uppercase alphabetic plus underscores,
     * length >= 2. Excludes operator and punctuation literals ({@code '='}, {@code ','}, etc.).
     */
    private static Set<String> deriveKeywordLiteralsFromLexer() {
        var vocab = SQL4JsonLexer.VOCABULARY;
        Set<String> out = new HashSet<>();
        for (int i = 1; i <= vocab.getMaxTokenType(); i++) {
            String literal = vocab.getLiteralName(i);
            if (literal == null) continue;
            // literal comes wrapped in quotes: "'SELECT'"
            String unquoted = literal.substring(1, literal.length() - 1);
            if (unquoted.length() >= 2 && unquoted.matches("[A-Z_]+")) {
                out.add(unquoted);
            }
        }
        // AGG_FUNCTION is defined as a single rule matching any of these five literals
        // (AVG|SUM|COUNT|MIN|MAX); the vocabulary has no literalName for union rules,
        // so include them explicitly. Same is true of ORDER_DIRECTION (ASC|DESC) and
        // BOOLEAN (TRUE|FALSE).
        out.addAll(List.of("AVG", "SUM", "COUNT", "MIN", "MAX", "ASC", "DESC", "TRUE", "FALSE"));
        return out;
    }
}
