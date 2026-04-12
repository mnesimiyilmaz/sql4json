package io.github.mnesimiyilmaz.sql4json.exception;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.*;

class ExceptionHierarchyTest {

    @Test
    void sql4JsonException_isSealed() {
        assertTrue(SQL4JsonException.class.isSealed(),
                "SQL4JsonException must be sealed");
        var permitted = SQL4JsonException.class.getPermittedSubclasses();
        assertEquals(2, permitted.length);
    }

    @Test
    void parseException_isFinal() {
        assertTrue(Modifier.isFinal(SQL4JsonParseException.class.getModifiers()),
                "SQL4JsonParseException must be final");
    }

    @Test
    void executionException_isFinal() {
        assertTrue(Modifier.isFinal(SQL4JsonExecutionException.class.getModifiers()),
                "SQL4JsonExecutionException must be final");
    }

    @Test
    void parseException_supportsCauseChain() {
        var cause = new RuntimeException("root cause");
        var ex = new SQL4JsonParseException("parse failed", 1, 5, cause);
        assertEquals("parse failed", ex.getMessage());
        assertEquals(1, ex.getLine());
        assertEquals(5, ex.getCharPosition());
        assertSame(cause, ex.getCause());
    }

    @Test
    void parseException_existingConstructor_stillWorks() {
        var ex = new SQL4JsonParseException("msg", 2, 10);
        assertEquals("msg", ex.getMessage());
        assertNull(ex.getCause());
    }
}
