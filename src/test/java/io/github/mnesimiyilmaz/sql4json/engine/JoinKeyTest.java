// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.engine;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import java.util.List;
import org.junit.jupiter.api.Test;

class JoinKeyTest {

    @Test
    void equal_keys_are_equal() {
        var k1 = new JoinKey(List.of(SqlNumber.of(1.0), new SqlString("a")));
        var k2 = new JoinKey(List.of(SqlNumber.of(1.0), new SqlString("a")));
        assertEquals(k1, k2);
        assertEquals(k1.hashCode(), k2.hashCode());
    }

    @Test
    void different_keys_are_not_equal() {
        var k1 = new JoinKey(List.of(SqlNumber.of(1.0)));
        var k2 = new JoinKey(List.of(SqlNumber.of(2.0)));
        assertNotEquals(k1, k2);
    }

    @Test
    void numeric_normalization_matches_int_and_double() {
        var k1 = JoinKey.of(List.of(SqlNumber.of(1)));
        var k2 = JoinKey.of(List.of(SqlNumber.of(1.0)));
        assertEquals(k1, k2);
        assertEquals(k1.hashCode(), k2.hashCode());
    }

    @Test
    void null_key_values() {
        var k1 = JoinKey.of(List.of(SqlNull.INSTANCE));
        var k2 = JoinKey.of(List.of(SqlNull.INSTANCE));
        assertEquals(k1, k2);
    }

    @Test
    void values_are_immutable() {
        var k = new JoinKey(List.of(SqlNumber.of(1.0)));
        var values = k.values();
        assertThrows(UnsupportedOperationException.class, () -> values.add(SqlNumber.of(2.0)));
    }
}
