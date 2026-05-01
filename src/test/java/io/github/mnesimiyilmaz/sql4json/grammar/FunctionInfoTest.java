package io.github.mnesimiyilmaz.sql4json.grammar;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FunctionInfoTest {

    @Test
    void componentsAreExposedAsRecordAccessors() {
        var info = new FunctionInfo("lower", Category.STRING, 1, 2, "LOWER(s, locale?)", "Lower-cases a string");
        assertEquals("lower", info.name());
        assertEquals(Category.STRING, info.category());
        assertEquals(1, info.minArity());
        assertEquals(2, info.maxArity());
        assertEquals("LOWER(s, locale?)", info.signature());
        assertEquals("Lower-cases a string", info.description());
    }

    @Test
    void varargIsExpressedAsMinusOneMaxArity() {
        var info = new FunctionInfo("concat", Category.STRING, 1, -1, "CONCAT(s, ...)", "Concatenates strings");
        assertEquals(-1, info.maxArity());
    }
}
