// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.binding;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import io.github.mnesimiyilmaz.sql4json.parser.ParameterConverter;
import io.github.mnesimiyilmaz.sql4json.types.SqlDecimal;
import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class ParameterConverterTest {

    @Test
    void byteValue_convertsToSqlNumberAsLong() {
        SqlValue v = ParameterConverter.toSqlValue((byte) 7);
        assertInstanceOf(SqlNumber.class, v);
        assertEquals(7L, ((SqlNumber) v).longValue());
    }

    @Test
    void shortValue_convertsToSqlNumberAsLong() {
        SqlValue v = ParameterConverter.toSqlValue((short) 1234);
        assertInstanceOf(SqlNumber.class, v);
        assertEquals(1234L, ((SqlNumber) v).longValue());
    }

    @Test
    void bigInteger_overflowingLong_isStoredAsBigDecimal() {
        BigInteger huge = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TEN);
        SqlValue v = ParameterConverter.toSqlValue(huge);
        assertInstanceOf(SqlDecimal.class, v);
        // Identity is no longer preserved (BigInteger → BigDecimal); compare by value.
        assertEquals(new BigDecimal(huge), ((SqlNumber) v).bigDecimalValue());
    }

    @Test
    void unsupportedType_throwsBindException() {
        assertThrows(SQL4JsonBindException.class, () -> ParameterConverter.toSqlValue(new Object()));
    }
}
