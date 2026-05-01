package io.github.mnesimiyilmaz.sql4json.cli;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ParamValueParserTest {

    @Test
    void parse_stringLiteral_returnsString() {
        Map.Entry<String, Object> e = ParamValueParser.parse("name=\"alice\"");
        assertEquals("name", e.getKey());
        assertEquals("alice", e.getValue());
    }

    @Test
    void parse_integerLiteral_returnsNumber() {
        Map.Entry<String, Object> e = ParamValueParser.parse("age=25");
        assertEquals("age", e.getKey());
        assertTrue(e.getValue() instanceof Number);
        assertEquals(25, ((Number) e.getValue()).intValue());
    }

    @Test
    void parse_fractionalLiteral_returnsDouble() {
        Map.Entry<String, Object> e = ParamValueParser.parse("price=19.99");
        assertEquals("price", e.getKey());
        // JsonParser routes any literal with a fractional part to JsonDoubleValue,
        // so numberValue() returns a boxed Double. BoundParameters accepts both
        // Double and BigDecimal — engine-side coercion handles either.
        assertTrue(e.getValue() instanceof Number);
        assertEquals(19.99, ((Number) e.getValue()).doubleValue(), 1e-9);
    }

    @Test
    void parse_overLongIntegerLiteral_returnsBigDecimal() {
        // Integer literals that overflow Long are routed to JsonDecimalValue.
        Map.Entry<String, Object> e = ParamValueParser.parse("big=99999999999999999999");
        assertEquals("big", e.getKey());
        assertEquals(new BigDecimal("99999999999999999999"), e.getValue());
    }

    @Test
    void parse_booleanTrue_returnsTrue() {
        assertEquals(Boolean.TRUE, ParamValueParser.parse("active=true").getValue());
    }

    @Test
    void parse_booleanFalse_returnsFalse() {
        assertEquals(Boolean.FALSE, ParamValueParser.parse("active=false").getValue());
    }

    @Test
    void parse_nullLiteral_returnsNull() {
        Map.Entry<String, Object> e = ParamValueParser.parse("v=null");
        assertEquals("v", e.getKey());
        assertNull(e.getValue());
    }

    @Test
    void parse_arrayLiteral_returnsList() {
        Map.Entry<String, Object> e = ParamValueParser.parse("ids=[1,2,3]");
        assertEquals("ids", e.getKey());
        assertTrue(e.getValue() instanceof List);
        List<?> list = (List<?>) e.getValue();
        assertEquals(3, list.size());
        assertEquals(1, ((Number) list.get(0)).intValue());
        assertEquals(2, ((Number) list.get(1)).intValue());
        assertEquals(3, ((Number) list.get(2)).intValue());
    }

    @Test
    void parse_arrayWithMixedTypes_returnsHeterogeneousList() {
        Map.Entry<String, Object> e = ParamValueParser.parse("xs=[1,\"two\",true,null]");
        List<?> list = (List<?>) e.getValue();
        assertEquals(4, list.size());
        assertEquals(1, ((Number) list.get(0)).intValue());
        assertEquals("two", list.get(1));
        assertEquals(Boolean.TRUE, list.get(2));
        assertNull(list.get(3));
    }

    @Test
    void parse_emptyArray_returnsEmptyList() {
        assertEquals(List.of(), ParamValueParser.parse("xs=[]").getValue());
    }

    @Test
    void parse_valueContainingEqualsSign_splitsOnFirstEquals() {
        Map.Entry<String, Object> e = ParamValueParser.parse("expr=\"a=b=c\"");
        assertEquals("expr", e.getKey());
        assertEquals("a=b=c", e.getValue());
    }

    @Test
    void parse_objectLiteral_throwsUsage() {
        UsageException ex = assertThrows(UsageException.class,
                () -> ParamValueParser.parse("o={\"k\":1}"));
        assertTrue(ex.getMessage().contains("'o'"));
        assertTrue(ex.getMessage().contains("object"));
    }

    @Test
    void parse_missingEquals_throwsUsage() {
        UsageException ex = assertThrows(UsageException.class,
                () -> ParamValueParser.parse("noequals"));
        assertTrue(ex.getMessage().contains("name=<json>"));
    }

    @Test
    void parse_emptyName_throwsUsage() {
        UsageException ex = assertThrows(UsageException.class,
                () -> ParamValueParser.parse("=42"));
        assertTrue(ex.getMessage().contains("name"));
    }

    @Test
    void parse_emptyValue_throwsUsage() {
        UsageException ex = assertThrows(UsageException.class,
                () -> ParamValueParser.parse("name="));
        assertTrue(ex.getMessage().contains("'name'"));
    }

    @Test
    void parse_malformedJsonValue_throwsUsage() {
        UsageException ex = assertThrows(UsageException.class,
                () -> ParamValueParser.parse("name=not-valid-json"));
        assertTrue(ex.getMessage().contains("'name'"));
    }

    @Test
    void parse_nullArg_throwsUsage() {
        assertThrows(UsageException.class, () -> ParamValueParser.parse(null));
    }
}
