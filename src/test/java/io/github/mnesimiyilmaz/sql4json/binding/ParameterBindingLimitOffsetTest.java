// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.binding;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.BoundParameters;
import io.github.mnesimiyilmaz.sql4json.engine.ParameterSubstitutor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class ParameterBindingLimitOffsetTest {

    private static final Sql4jsonSettings S = Sql4jsonSettings.defaults();

    private static QueryDefinition substituteLimit(String sql, Object limitValue) {
        QueryDefinition def = QueryParser.parse(sql, S);
        return ParameterSubstitutor.substitute(def, BoundParameters.of(limitValue), S);
    }

    @Test
    void when_named_limit_then_resolved_to_int() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r LIMIT :n", S);
        QueryDefinition out =
                ParameterSubstitutor.substitute(def, BoundParameters.named().bind("n", 10), S);
        assertEquals(Integer.valueOf(10), out.limit());
        assertNull(out.limitParam());
    }

    @Test
    void when_positional_limit_and_offset_then_resolved() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r LIMIT ? OFFSET ?", S);
        QueryDefinition out = ParameterSubstitutor.substitute(def, BoundParameters.of(2, 4), S);
        assertEquals(Integer.valueOf(2), out.limit());
        assertEquals(Integer.valueOf(4), out.offset());
    }

    @Test
    void when_limit_bound_null_then_exception() {
        var e = assertThrows(SQL4JsonBindException.class, () -> substituteLimit("SELECT * FROM $r LIMIT ?", null));
        assertTrue(e.getMessage().contains("LIMIT"));
    }

    @Test
    void when_limit_bound_negative_then_exception() {
        assertThrows(SQL4JsonBindException.class, () -> substituteLimit("SELECT * FROM $r LIMIT ?", -1));
    }

    @Test
    void when_limit_bound_string_then_exception() {
        assertThrows(SQL4JsonBindException.class, () -> substituteLimit("SELECT * FROM $r LIMIT ?", "ten"));
    }

    @Test
    void when_limit_bound_fractional_big_decimal_then_exception() {
        assertThrows(
                SQL4JsonBindException.class, () -> substituteLimit("SELECT * FROM $r LIMIT ?", new BigDecimal("3.14")));
    }

    @Test
    void when_limit_bound_integer_big_decimal_then_ok() {
        QueryDefinition out = substituteLimit("SELECT * FROM $r LIMIT ?", new BigDecimal("2.00"));
        assertEquals(Integer.valueOf(2), out.limit());
    }

    @Test
    void when_limit_bound_big_integer_then_ok() {
        QueryDefinition out = substituteLimit("SELECT * FROM $r LIMIT ?", new BigInteger("5"));
        assertEquals(Integer.valueOf(5), out.limit());
    }

    @Test
    void when_limit_bound_big_integer_overflow_then_exception() {
        BigInteger huge = BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.TEN);
        assertThrows(SQL4JsonBindException.class, () -> substituteLimit("SELECT * FROM $r LIMIT ?", huge));
    }

    @Test
    void when_limit_bound_double_then_exception() {
        assertThrows(SQL4JsonBindException.class, () -> substituteLimit("SELECT * FROM $r LIMIT ?", 2.5));
    }

    @Test
    void when_limit_bound_float_then_exception() {
        assertThrows(SQL4JsonBindException.class, () -> substituteLimit("SELECT * FROM $r LIMIT ?", 2.5f));
    }

    @Test
    void when_limit_bound_long_within_int_range_then_ok() {
        QueryDefinition out = substituteLimit("SELECT * FROM $r LIMIT ?", 5_000L);
        assertEquals(Integer.valueOf(5_000), out.limit());
    }

    @Test
    void when_limit_bound_long_out_of_int_range_then_exception() {
        long big = (long) Integer.MAX_VALUE + 1L;
        assertThrows(SQL4JsonBindException.class, () -> substituteLimit("SELECT * FROM $r LIMIT ?", big));
    }

    @Test
    void when_limit_literal_and_offset_parameter_mixed_then_works() {
        QueryDefinition def = QueryParser.parse("SELECT * FROM $r LIMIT 2 OFFSET ?", S);
        QueryDefinition out = ParameterSubstitutor.substitute(def, BoundParameters.of(3), S);
        assertEquals(Integer.valueOf(2), out.limit());
        assertEquals(Integer.valueOf(3), out.offset());
    }
}
