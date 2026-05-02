// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.grammar;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class SQL4JsonGrammarKeywordsTest {

    @Test
    void containsCoreKeywords() {
        List<String> kw = SQL4JsonGrammar.keywords();
        assertTrue(kw.contains("SELECT"));
        assertTrue(kw.contains("FROM"));
        assertTrue(kw.contains("WHERE"));
        assertTrue(kw.contains("GROUP"));
        assertTrue(kw.contains("ORDER"));
        assertTrue(kw.contains("HAVING"));
    }

    @Test
    void containsWindowFunctionNames() {
        List<String> kw = SQL4JsonGrammar.keywords();
        assertTrue(kw.contains("ROW_NUMBER"));
        assertTrue(kw.contains("DENSE_RANK"));
        assertTrue(kw.contains("NTILE"));
        assertTrue(kw.contains("LAG"));
        assertTrue(kw.contains("LEAD"));
    }

    @Test
    void containsAggregateFunctionTokens() {
        // AGG_FUNCTION is a single lexer rule matching AVG|SUM|COUNT|MIN|MAX; all five
        // spellings belong in the keywords catalog for completion.
        List<String> kw = SQL4JsonGrammar.keywords();
        assertTrue(kw.containsAll(List.of("AVG", "SUM", "COUNT", "MIN", "MAX")));
    }

    @Test
    void doesNotContainOperatorsOrPunctuation() {
        List<String> kw = SQL4JsonGrammar.keywords();
        assertFalse(kw.contains("="));
        assertFalse(kw.contains(","));
        assertFalse(kw.contains("*"));
        assertFalse(kw.contains("("));
    }

    @Test
    void isSortedAndDeduplicated() {
        List<String> kw = SQL4JsonGrammar.keywords();
        List<String> sorted = kw.stream().sorted().distinct().toList();
        assertEquals(sorted, kw);
    }

    @Test
    void isUnmodifiable() {
        List<String> kw = SQL4JsonGrammar.keywords();
        assertThrows(UnsupportedOperationException.class, () -> kw.add("HACK"));
    }
}
