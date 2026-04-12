package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.engine.Expression;
import io.github.mnesimiyilmaz.sql4json.engine.Expression.ColumnRef;
import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.Row;
import io.github.mnesimiyilmaz.sql4json.json.JsonNumberValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonStringValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

class LikeConditionHandlerTest {

    private final LikeConditionHandler handler = new LikeConditionHandler(new BoundedPatternCache(256));

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static Row rowWithString(String field, String value) {
        var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        fields.put(field, new JsonStringValue(value));
        return Row.lazy(new JsonObjectValue(fields), new FieldKey.Interner());
    }

    private static Row rowWithNumber(String field, int value) {
        var fields = new LinkedHashMap<String, io.github.mnesimiyilmaz.sql4json.types.JsonValue>();
        fields.put(field, new JsonNumberValue(value));
        return Row.lazy(new JsonObjectValue(fields), new FieldKey.Interner());
    }

    private static ConditionContext likeCtx(String col, String pattern) {
        SqlString patternVal = new SqlString(pattern);
        return new ConditionContext(ConditionContext.ConditionType.LIKE,
                new ColumnRef(col), "LIKE", patternVal,
                new Expression.LiteralVal(patternVal), null, null, null);
    }

    private CriteriaNode nodeFor(String col, String pattern) {
        return handler.handle(likeCtx(col, pattern), null, null);
    }

    // ── canHandle ────────────────────────────────────────────────────────────

    @Test
    void canHandle_like_true() {
        assertTrue(handler.canHandle(likeCtx("name", "A%")));
    }

    @Test
    void canHandle_comparison_false() {
        ConditionContext ctx = new ConditionContext(ConditionContext.ConditionType.COMPARISON,
                new ColumnRef("age"), ">", SqlNumber.of(1),
                new Expression.LiteralVal(SqlNumber.of(1)), null, null, null);
        assertFalse(handler.canHandle(ctx));
    }

    // ── Wildcard: % (zero or more chars) ─────────────────────────────────────

    @Test
    void trailingPercent_matchesPrefix() {
        CriteriaNode node = nodeFor("name", "Al%");
        assertTrue(node.test(rowWithString("name", "Alice")));
        assertTrue(node.test(rowWithString("name", "Alan")));
        assertTrue(node.test(rowWithString("name", "Al")));     // zero extra chars
        assertFalse(node.test(rowWithString("name", "Bob")));
    }

    @Test
    void leadingPercent_matchesSuffix() {
        CriteriaNode node = nodeFor("name", "%son");
        assertTrue(node.test(rowWithString("name", "Jackson")));
        assertTrue(node.test(rowWithString("name", "Wilson")));
        assertFalse(node.test(rowWithString("name", "Alice")));
    }

    @Test
    void bothSidesPercent_matchesSubstring() {
        CriteriaNode node = nodeFor("name", "%lic%");
        assertTrue(node.test(rowWithString("name", "Alice")));
        assertTrue(node.test(rowWithString("name", "Felicia")));
        assertFalse(node.test(rowWithString("name", "Bob")));
    }

    @Test
    void onlyPercent_matchesAnything() {
        CriteriaNode node = nodeFor("value", "%");
        assertTrue(node.test(rowWithString("value", "")));
        assertTrue(node.test(rowWithString("value", "anything here")));
    }

    // ── Wildcard: _ (exactly one char) ───────────────────────────────────────

    @Test
    void underscore_matchesExactlyOneChar() {
        CriteriaNode node = nodeFor("name", "_lice");
        assertTrue(node.test(rowWithString("name", "Alice")));
        assertTrue(node.test(rowWithString("name", "Blice")));
        assertFalse(node.test(rowWithString("name", "Alice2")));  // extra char
        assertFalse(node.test(rowWithString("name", "lice")));    // no leading char
    }

    @Test
    void multipleUnderscores() {
        CriteriaNode node = nodeFor("code", "A__");
        assertTrue(node.test(rowWithString("code", "A12")));
        assertTrue(node.test(rowWithString("code", "AXY")));
        assertFalse(node.test(rowWithString("code", "A1")));   // too short
        assertFalse(node.test(rowWithString("code", "A123"))); // too long
    }

    // ── No wildcard: exact match ──────────────────────────────────────────────

    @Test
    void noWildcard_exactMatch_true() {
        CriteriaNode node = nodeFor("status", "active");
        assertTrue(node.test(rowWithString("status", "active")));
    }

    @Test
    void noWildcard_exactMatch_false() {
        CriteriaNode node = nodeFor("status", "active");
        assertFalse(node.test(rowWithString("status", "inactive")));
    }

    // ── ReDoS safety: regex metacharacters treated as literals ────────────────

    @Test
    void dotInPattern_treatedAsLiteralNotRegexWildcard() {
        CriteriaNode node = nodeFor("price", "3.14");
        assertTrue(node.test(rowWithString("price", "3.14")));
        assertFalse(node.test(rowWithString("price", "3X14")),
                "Dot in LIKE pattern must be literal, not a regex wildcard");
        assertFalse(node.test(rowWithString("price", "3014")));
    }

    @Test
    void bracketInPattern_noException() {
        CriteriaNode node = nodeFor("code", "[abc]");
        assertDoesNotThrow(() -> node.test(rowWithString("code", "[abc]")));
        assertTrue(node.test(rowWithString("code", "[abc]")));
        assertFalse(node.test(rowWithString("code", "a")),
                "Brackets must be literal in LIKE pattern");
    }

    @Test
    void plusInPattern_treatedAsLiteral() {
        CriteriaNode node = nodeFor("value", "1+1");
        assertTrue(node.test(rowWithString("value", "1+1")));
        assertFalse(node.test(rowWithString("value", "11")),
                "Plus in LIKE pattern must be literal, not regex quantifier");
    }

    @Test
    void asteriskInPattern_treatedAsLiteral() {
        CriteriaNode node = nodeFor("expr", "a*b");
        assertTrue(node.test(rowWithString("expr", "a*b")));
        assertFalse(node.test(rowWithString("expr", "ab")),
                "Asterisk in LIKE pattern must be literal");
    }

    @Test
    void parenInPattern_noException() {
        CriteriaNode node = nodeFor("note", "(test)");
        assertDoesNotThrow(() -> node.test(rowWithString("note", "(test)")));
        assertTrue(node.test(rowWithString("note", "(test)")));
    }

    @Test
    void dollarSignInPattern_treatedAsLiteral() {
        CriteriaNode node = nodeFor("note", "$100");
        assertTrue(node.test(rowWithString("note", "$100")));
        assertFalse(node.test(rowWithString("note", "100")));
    }

    // ── Combined: literal + wildcards ────────────────────────────────────────

    @Test
    void literalDotWithPercent_correctBehavior() {
        // "1.%" matches "1.0", "1.23", "1." but NOT "1X0"
        CriteriaNode node = nodeFor("version", "1.%");
        assertTrue(node.test(rowWithString("version", "1.0")));
        assertTrue(node.test(rowWithString("version", "1.23")));
        assertFalse(node.test(rowWithString("version", "1X0")),
                "Dot before % must be literal");
    }

    // ── Case insensitivity ────────────────────────────────────────────────────

    @Test
    void like_caseInsensitive() {
        CriteriaNode node = nodeFor("name", "alice%");
        assertTrue(node.test(rowWithString("name", "Alice")));
        assertTrue(node.test(rowWithString("name", "ALICE123")));
        assertTrue(node.test(rowWithString("name", "alice")));
    }

    // ── Non-string column value ───────────────────────────────────────────────

    @Test
    void nonStringColumnValue_returnsFalse() {
        CriteriaNode node = nodeFor("age", "25%");
        assertFalse(node.test(rowWithNumber("age", 25)),
                "LIKE on a non-string value should return false");
    }

    // ── NULL column value ─────────────────────────────────────────────────────

    @Test
    void nullColumnValue_returnsFalse() {
        CriteriaNode node = nodeFor("name", "A%");
        Row row = Row.eager(java.util.Map.of()); // "name" absent → SqlNull
        assertFalse(node.test(row), "LIKE on SqlNull should return false");
    }
}
