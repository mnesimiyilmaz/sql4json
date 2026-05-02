// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.grammar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import org.junit.jupiter.api.Test;

/**
 * Confirms that moving {@code ESC} to {@code channel(HIDDEN)} leaves the parser unaffected for representative
 * multi-line queries. If this test fails after the channel change, {@code CommonTokenStream}'s default filter is not
 * handling hidden whitespace as expected.
 */
class SQL4JsonGrammarChannelRegressionTest {

    @Test
    void multiLineSelectWithVariedWhitespaceStillParses() {
        String sql = """
                SELECT   name ,
                         age
                   FROM    $r
                  WHERE age  >  25
                  GROUP BY  dept
                  HAVING    COUNT(*)  >  1
                  ORDER BY  name  DESC
                """;
        var qd = QueryParser.parse(sql);
        assertNotNull(qd);
        assertEquals(2, qd.selectedColumns().size());
        assertNotNull(qd.whereClause());
        assertNotNull(qd.groupBy());
        assertNotNull(qd.havingClause());
        assertNotNull(qd.orderBy());
    }

    @Test
    void tabsAndCarriageReturnsAreAcceptedAsWhitespace() {
        String sql = "SELECT\ta\r\nFROM\t$r";
        var qd = QueryParser.parse(sql);
        assertNotNull(qd);
        assertEquals(1, qd.selectedColumns().size());
    }
}
