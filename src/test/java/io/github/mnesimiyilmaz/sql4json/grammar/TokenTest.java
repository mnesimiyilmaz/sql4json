package io.github.mnesimiyilmaz.sql4json.grammar;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TokenTest {

    @Test
    void componentsAreExposedAsRecordAccessors() {
        var t = new Token(TokenKind.KEYWORD, 0, 6);
        assertEquals(TokenKind.KEYWORD, t.kind());
        assertEquals(0, t.startOffset());
        assertEquals(6, t.endOffset());
    }

    @Test
    void equalityIsValueBased() {
        var a = new Token(TokenKind.IDENTIFIER, 7, 8);
        var b = new Token(TokenKind.IDENTIFIER, 7, 8);
        var c = new Token(TokenKind.KEYWORD, 7, 8);
        assertEquals(a, b);
        assertNotEquals(a, c);
    }
}
