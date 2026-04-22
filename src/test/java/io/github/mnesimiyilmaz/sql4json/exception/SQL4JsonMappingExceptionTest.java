package io.github.mnesimiyilmaz.sql4json.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SQL4JsonMappingExceptionTest {

    @Test
    void when_created_with_message_then_is_subtype_of_base() {
        SQL4JsonMappingException e = new SQL4JsonMappingException("msg");
        assertEquals("msg", e.getMessage());
        assertInstanceOf(SQL4JsonException.class, e);
        assertInstanceOf(RuntimeException.class, e);
    }

    @Test
    void when_created_with_cause_then_cause_is_preserved() {
        Throwable cause = new IllegalArgumentException("root");
        SQL4JsonMappingException e = new SQL4JsonMappingException("msg", cause);
        assertSame(cause, e.getCause());
    }
}
