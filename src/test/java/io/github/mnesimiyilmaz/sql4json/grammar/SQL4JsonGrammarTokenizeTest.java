// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.grammar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class SQL4JsonGrammarTokenizeTest {

    @Test
    void emptyInputYieldsEmptyList() {
        assertTrue(SQL4JsonGrammar.tokenize("").isEmpty());
    }

    @Test
    void selectIdentifierYieldsKeywordWhitespaceIdentifier() {
        List<Token> tokens = SQL4JsonGrammar.tokenize("SELECT a");
        assertEquals(3, tokens.size());
        assertEquals(TokenKind.KEYWORD, tokens.get(0).kind());
        assertEquals(TokenKind.WHITESPACE, tokens.get(1).kind());
        assertEquals(TokenKind.IDENTIFIER, tokens.get(2).kind());
        // Offsets
        assertEquals(0, tokens.get(0).startOffset());
        assertEquals(6, tokens.get(0).endOffset());
        assertEquals(6, tokens.get(1).startOffset());
        assertEquals(7, tokens.get(1).endOffset());
        assertEquals(7, tokens.get(2).startOffset());
        assertEquals(8, tokens.get(2).endOffset());
    }

    @Test
    void dollarRIsRootRef() {
        List<Token> tokens = SQL4JsonGrammar.tokenize("$r");
        assertEquals(1, tokens.size());
        assertEquals(TokenKind.ROOT_REF, tokens.getFirst().kind());
    }

    @Test
    void stringLiteralIncludesDelimiters() {
        List<Token> tokens = SQL4JsonGrammar.tokenize("'hi'");
        assertEquals(1, tokens.size());
        Token t = tokens.getFirst();
        assertEquals(TokenKind.STRING_LITERAL, t.kind());
        assertEquals(0, t.startOffset());
        assertEquals(4, t.endOffset());
    }

    @Test
    void numericLiteral() {
        List<Token> tokens = SQL4JsonGrammar.tokenize("42");
        assertEquals(1, tokens.size());
        assertEquals(TokenKind.NUMBER_LITERAL, tokens.getFirst().kind());
    }

    @Test
    void positionalAndNamedParameters() {
        assertEquals(
                TokenKind.PARAM_POSITIONAL,
                SQL4JsonGrammar.tokenize("?").getFirst().kind());
        assertEquals(
                TokenKind.PARAM_NAMED, SQL4JsonGrammar.tokenize(":x").getFirst().kind());
    }

    @Test
    void badCharacterIsReportedWithoutThrowing() {
        // `@` has no lexer rule — the lexer produces a syntax error via its default
        // listener (we remove it) and skips the character; we fill the gap with BAD_TOKEN.
        List<Token> tokens = SQL4JsonGrammar.tokenize("SELECT @");
        boolean hasBad = tokens.stream().anyMatch(t -> t.kind() == TokenKind.BAD_TOKEN);
        assertTrue(hasBad);
    }

    @Test
    void nowParensAreIdentifierAndPunctuationAfterC2() {
        // Post-refactor: no dedicated VALUE_FUNCTION token; NOW() lexes as IDENTIFIER + ( + ).
        List<Token> tokens = SQL4JsonGrammar.tokenize("NOW()");
        assertEquals(3, tokens.size());
        assertEquals(TokenKind.IDENTIFIER, tokens.get(0).kind());
        assertEquals(TokenKind.PUNCTUATION, tokens.get(1).kind());
        assertEquals(TokenKind.PUNCTUATION, tokens.get(2).kind());
    }

    @Test
    void offsetsAreContiguousAndSorted() {
        List<Token> tokens = SQL4JsonGrammar.tokenize("SELECT a FROM $r WHERE x = 1");
        for (int i = 0; i < tokens.size(); i++) {
            Token t = tokens.get(i);
            assertTrue(t.startOffset() <= t.endOffset(), "negative-length token " + t);
            if (i > 0) {
                assertEquals(
                        tokens.get(i - 1).endOffset(), t.startOffset(), "gap between tokens " + (i - 1) + " and " + i);
            }
        }
    }

    @Test
    void asteriskIsOperator() {
        List<Token> tokens = SQL4JsonGrammar.tokenize("SELECT *");
        // [KEYWORD("SELECT"), WHITESPACE(" "), OPERATOR("*")]
        assertEquals(3, tokens.size());
        assertEquals(TokenKind.OPERATOR, tokens.get(2).kind());
    }

    @Test
    void tokenize_arrayOperators_recognisedAsOperatorTokens() {
        String sql = "tags @> ARRAY ['admin']";
        List<Token> tokens = SQL4JsonGrammar.tokenize(sql);
        List<TokenKind> kinds = tokens.stream().map(Token::kind).toList();
        assertTrue(kinds.contains(TokenKind.OPERATOR), "expected at least one OPERATOR token");
        assertTrue(kinds.contains(TokenKind.KEYWORD), "expected at least one KEYWORD token");
        List<String> operatorTokens = tokens.stream()
                .filter(t -> t.kind() == TokenKind.OPERATOR)
                .map(t -> sql.substring(t.startOffset(), t.endOffset()))
                .toList();
        assertTrue(operatorTokens.contains("@>"), "expected '@>' among operator slices, got " + operatorTokens);
        List<String> keywordTokens = tokens.stream()
                .filter(t -> t.kind() == TokenKind.KEYWORD)
                .map(t -> sql.substring(t.startOffset(), t.endOffset()).toUpperCase())
                .toList();
        assertTrue(keywordTokens.contains("ARRAY"), "expected 'ARRAY' among keyword slices, got " + keywordTokens);
    }

    @Test
    void tokenize_containsKeyword_recognisedAsKeyword() {
        String sql = "tags CONTAINS 'admin'";
        List<Token> tokens = SQL4JsonGrammar.tokenize(sql);
        List<String> keywordSlices = tokens.stream()
                .filter(t -> t.kind() == TokenKind.KEYWORD)
                .map(t -> sql.substring(t.startOffset(), t.endOffset()).toUpperCase())
                .toList();
        assertTrue(
                keywordSlices.contains("CONTAINS"), "expected 'CONTAINS' among keyword slices, got " + keywordSlices);
    }

    @Test
    void tokenize_containedByAndOverlapOperators_recognised() {
        String sql = "a <@ ARRAY [1] && ARRAY [2]";
        List<Token> tokens = SQL4JsonGrammar.tokenize(sql);
        List<String> operatorSlices = tokens.stream()
                .filter(t -> t.kind() == TokenKind.OPERATOR)
                .map(t -> sql.substring(t.startOffset(), t.endOffset()))
                .toList();
        assertTrue(operatorSlices.contains("<@"), "expected '<@' among operator slices, got " + operatorSlices);
        assertTrue(operatorSlices.contains("&&"), "expected '&&' among operator slices, got " + operatorSlices);
    }
}
