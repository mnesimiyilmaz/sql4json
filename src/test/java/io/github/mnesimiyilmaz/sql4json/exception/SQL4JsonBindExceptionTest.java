// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.exception;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SQL4JsonBindExceptionTest {

    @Test
    void when_created_with_message_then_is_subtype_of_base() {
        SQL4JsonBindException e = new SQL4JsonBindException("msg");
        assertEquals("msg", e.getMessage());
        assertInstanceOf(SQL4JsonException.class, e);
        assertInstanceOf(RuntimeException.class, e);
    }

    @Test
    void when_created_with_cause_then_cause_is_preserved() {
        Throwable cause = new IllegalArgumentException("root");
        SQL4JsonBindException e = new SQL4JsonBindException("msg", cause);
        assertSame(cause, e.getCause());
    }
}
