package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FunctionUnitTest {

    private static FunctionRegistry registry;

    @BeforeAll
    static void setup() {
        registry = FunctionRegistry.createDefault();
    }

    private static SqlValue apply(String fnName, SqlValue val, SqlValue... args) {
        return registry.getScalar(fnName)
                .orElseThrow(() -> new AssertionError("Function not found: " + fnName))
                .apply().apply(val, List.of(args));
    }

    @Nested
    class StringFunctions {

        @Test
        void concat_twoStrings() {
            assertEquals(new SqlString("helloworld"),
                    apply("concat", new SqlString("hello"), new SqlString("world")));
        }

        @Test
        void concat_mixedTypes() {
            var result = apply("concat", new SqlString("age"), SqlNumber.of(25));
            assertInstanceOf(SqlString.class, result);
            assertTrue(((SqlString) result).value().startsWith("age"));
        }

        @Test
        void concat_nullInput() {
            assertEquals(SqlNull.INSTANCE, apply("concat", SqlNull.INSTANCE, new SqlString("x")));
        }

        @Test
        void concat_nullArg() {
            assertEquals(SqlNull.INSTANCE, apply("concat", new SqlString("hello"), SqlNull.INSTANCE));
        }

        @Test
        void substring_basic() {
            assertEquals(new SqlString("ell"),
                    apply("substring", new SqlString("hello"), SqlNumber.of(2), SqlNumber.of(3)));
        }

        @Test
        void substring_startBeyondLength() {
            assertEquals(new SqlString(""),
                    apply("substring", new SqlString("hi"), SqlNumber.of(10), SqlNumber.of(3)));
        }

        @Test
        void substring_lengthExceedsRemaining() {
            assertEquals(new SqlString("llo"),
                    apply("substring", new SqlString("hello"), SqlNumber.of(3), SqlNumber.of(100)));
        }

        @Test
        void substring_startLessThanOne() {
            assertEquals(new SqlString("he"),
                    apply("substring", new SqlString("hello"), SqlNumber.of(0), SqlNumber.of(3)));
        }

        @Test
        void substring_zeroLength() {
            assertEquals(new SqlString(""),
                    apply("substring", new SqlString("hello"), SqlNumber.of(1), SqlNumber.of(0)));
        }

        @Test
        void trim_basic() {
            assertEquals(new SqlString("hello"), apply("trim", new SqlString("  hello  ")));
        }

        @Test
        void length_basic() {
            assertEquals(SqlNumber.of(5), apply("length", new SqlString("hello")));
        }

        @Test
        void length_empty() {
            assertEquals(SqlNumber.of(0), apply("length", new SqlString("")));
        }

        @Test
        void replace_basic() {
            assertEquals(new SqlString("hXllo"),
                    apply("replace", new SqlString("hello"), new SqlString("e"), new SqlString("X")));
        }

        @Test
        void replace_allOccurrences() {
            assertEquals(new SqlString("XbXbX"),
                    apply("replace", new SqlString("ababa"), new SqlString("a"), new SqlString("X")));
        }

        @Test
        void left_basic() {
            assertEquals(new SqlString("hel"), apply("left", new SqlString("hello"), SqlNumber.of(3)));
        }

        @Test
        void left_beyondLength() {
            assertEquals(new SqlString("hi"), apply("left", new SqlString("hi"), SqlNumber.of(10)));
        }

        @Test
        void right_basic() {
            assertEquals(new SqlString("llo"), apply("right", new SqlString("hello"), SqlNumber.of(3)));
        }

        @Test
        void lpad_basic() {
            assertEquals(new SqlString("**hi"),
                    apply("lpad", new SqlString("hi"), SqlNumber.of(4), new SqlString("*")));
        }

        @Test
        void lpad_alreadyLong() {
            assertEquals(new SqlString("hel"),
                    apply("lpad", new SqlString("hello"), SqlNumber.of(3), new SqlString("*")));
        }

        @Test
        void rpad_basic() {
            assertEquals(new SqlString("hi**"),
                    apply("rpad", new SqlString("hi"), SqlNumber.of(4), new SqlString("*")));
        }

        @Test
        void reverse_basic() {
            assertEquals(new SqlString("olleh"), apply("reverse", new SqlString("hello")));
        }

        @Test
        void position_found() {
            assertEquals(SqlNumber.of(3),
                    apply("position", new SqlString("l"), new SqlString("hello")));
        }

        @Test
        void position_notFound() {
            assertEquals(SqlNumber.of(0),
                    apply("position", new SqlString("z"), new SqlString("hello")));
        }
    }

    @Nested
    class MathFunctions {

        @Test
        void abs_positive() {
            assertEquals(SqlNumber.of(5), apply("abs", SqlNumber.of(5)));
        }

        @Test
        void abs_negative() {
            assertEquals(SqlNumber.of(5), apply("abs", SqlNumber.of(-5)));
        }

        @Test
        void abs_null() {
            assertEquals(SqlNull.INSTANCE, apply("abs", SqlNull.INSTANCE));
        }

        @Test
        void round_noDecimals() {
            assertEquals(SqlNumber.of(3), apply("round", SqlNumber.of(2.7)));
        }

        @Test
        void round_withDecimals() {
            assertEquals(SqlNumber.of(2.73), apply("round", SqlNumber.of(2.725), SqlNumber.of(2)));
        }

        @Test
        void round_halfUp() {
            assertEquals(SqlNumber.of(3), apply("round", SqlNumber.of(2.5)));
        }

        @Test
        void ceil_basic() {
            assertEquals(SqlNumber.of(3), apply("ceil", SqlNumber.of(2.1)));
        }

        @Test
        void ceil_negative() {
            assertEquals(SqlNumber.of(-2), apply("ceil", SqlNumber.of(-2.9)));
        }

        @Test
        void floor_basic() {
            assertEquals(SqlNumber.of(2), apply("floor", SqlNumber.of(2.9)));
        }

        @Test
        void floor_negative() {
            assertEquals(SqlNumber.of(-3), apply("floor", SqlNumber.of(-2.1)));
        }

        @Test
        void mod_basic() {
            assertEquals(SqlNumber.of(1), apply("mod", SqlNumber.of(7), SqlNumber.of(3)));
        }

        @Test
        void mod_byZero() {
            assertEquals(SqlNull.INSTANCE, apply("mod", SqlNumber.of(5), SqlNumber.of(0)));
        }

        @Test
        void power_basic() {
            assertEquals(SqlNumber.of(8), apply("power", SqlNumber.of(2), SqlNumber.of(3)));
        }

        @Test
        void power_overflow() {
            assertEquals(SqlNull.INSTANCE, apply("power", SqlNumber.of(10), SqlNumber.of(309)));
        }

        @Test
        void sqrt_basic() {
            assertEquals(SqlNumber.of(3), apply("sqrt", SqlNumber.of(9)));
        }

        @Test
        void sqrt_negative() {
            assertEquals(SqlNull.INSTANCE, apply("sqrt", SqlNumber.of(-4)));
        }

        @Test
        void sign_positive() {
            assertEquals(SqlNumber.of(1), apply("sign", SqlNumber.of(42)));
        }

        @Test
        void sign_negative() {
            assertEquals(SqlNumber.of(-1), apply("sign", SqlNumber.of(-7)));
        }

        @Test
        void sign_zero() {
            assertEquals(SqlNumber.of(0), apply("sign", SqlNumber.of(0)));
        }
    }

    @Nested
    class DateFunctions {

        @Test
        void year_fromDate() {
            assertEquals(SqlNumber.of(2024), apply("year", new SqlDate(LocalDate.of(2024, 6, 15))));
        }

        @Test
        void month_fromDate() {
            assertEquals(SqlNumber.of(6), apply("month", new SqlDate(LocalDate.of(2024, 6, 15))));
        }

        @Test
        void day_fromDate() {
            assertEquals(SqlNumber.of(15), apply("day", new SqlDate(LocalDate.of(2024, 6, 15))));
        }

        @Test
        void year_fromDateTime() {
            assertEquals(SqlNumber.of(2024),
                    apply("year", new SqlDateTime(LocalDateTime.of(2024, 6, 15, 10, 30))));
        }

        @Test
        void hour_fromDateTime() {
            assertEquals(SqlNumber.of(10),
                    apply("hour", new SqlDateTime(LocalDateTime.of(2024, 6, 15, 10, 30, 45))));
        }

        @Test
        void minute_fromDateTime() {
            assertEquals(SqlNumber.of(30),
                    apply("minute", new SqlDateTime(LocalDateTime.of(2024, 6, 15, 10, 30, 45))));
        }

        @Test
        void second_fromDateTime() {
            assertEquals(SqlNumber.of(45),
                    apply("second", new SqlDateTime(LocalDateTime.of(2024, 6, 15, 10, 30, 45))));
        }

        @Test
        void hour_fromDate_returnsZero() {
            assertEquals(SqlNumber.of(0), apply("hour", new SqlDate(LocalDate.of(2024, 6, 15))));
        }

        @Test
        void dateAdd_days() {
            assertEquals(new SqlDate(LocalDate.of(2024, 6, 20)),
                    apply("date_add", new SqlDate(LocalDate.of(2024, 6, 15)),
                            SqlNumber.of(5), new SqlString("DAY")));
        }

        @Test
        void dateAdd_months() {
            assertEquals(new SqlDate(LocalDate.of(2024, 9, 15)),
                    apply("date_add", new SqlDate(LocalDate.of(2024, 6, 15)),
                            SqlNumber.of(3), new SqlString("MONTH")));
        }

        @Test
        void dateAdd_dateTime_preservesType() {
            var result = apply("date_add",
                    new SqlDateTime(LocalDateTime.of(2024, 6, 15, 10, 0)),
                    SqlNumber.of(2), new SqlString("HOUR"));
            assertInstanceOf(SqlDateTime.class, result);
            assertEquals(LocalDateTime.of(2024, 6, 15, 12, 0), ((SqlDateTime) result).value());
        }

        @Test
        void dateAdd_nullInput() {
            assertEquals(SqlNull.INSTANCE,
                    apply("date_add", SqlNull.INSTANCE, SqlNumber.of(1), new SqlString("DAY")));
        }

        @Test
        void dateDiff_days_positive() {
            assertEquals(SqlNumber.of(5),
                    apply("date_diff", new SqlDate(LocalDate.of(2024, 6, 20)),
                            new SqlDate(LocalDate.of(2024, 6, 15)), new SqlString("DAY")));
        }

        @Test
        void dateDiff_days_negative() {
            assertEquals(SqlNumber.of(-5),
                    apply("date_diff", new SqlDate(LocalDate.of(2024, 6, 15)),
                            new SqlDate(LocalDate.of(2024, 6, 20)), new SqlString("DAY")));
        }

        @Test
        void dateDiff_months() {
            assertEquals(SqlNumber.of(3),
                    apply("date_diff", new SqlDate(LocalDate.of(2024, 9, 15)),
                            new SqlDate(LocalDate.of(2024, 6, 15)), new SqlString("MONTH")));
        }
    }

    @Nested
    class NullIfAndCastFunctions {

        // --- NULLIF ---
        @Test
        void nullif_equal_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("nullif", new SqlString("a"), new SqlString("a")));
        }

        @Test
        void nullif_notEqual_returnsValue() {
            assertEquals(new SqlString("a"), apply("nullif", new SqlString("a"), new SqlString("b")));
        }

        @Test
        void nullif_nullInput_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("nullif", SqlNull.INSTANCE, new SqlString("a")));
        }

        // --- CAST to STRING ---
        @Test
        void cast_numberToString() {
            var result = apply("cast", SqlNumber.of(25), new SqlString("STRING"));
            assertInstanceOf(SqlString.class, result);
        }

        @Test
        void cast_boolToString() {
            assertEquals(new SqlString("true"), apply("cast", SqlBoolean.of(true), new SqlString("STRING")));
        }

        @Test
        void cast_dateToString() {
            assertEquals(new SqlString("2024-06-15"),
                    apply("cast", new SqlDate(LocalDate.of(2024, 6, 15)), new SqlString("STRING")));
        }

        // --- CAST to NUMBER ---
        @Test
        void cast_stringToNumber() {
            var result = apply("cast", new SqlString("42"), new SqlString("NUMBER"));
            assertInstanceOf(SqlNumber.class, result);
            assertEquals(42.0, ((SqlNumber) result).doubleValue());
        }

        @Test
        void cast_boolToNumber() {
            var result = apply("cast", SqlBoolean.of(true), new SqlString("NUMBER"));
            assertEquals(1.0, ((SqlNumber) result).doubleValue());
        }

        @Test
        void cast_invalidStringToNumber() {
            SqlString input = new SqlString("abc");
            SqlString targetType = new SqlString("NUMBER");
            assertThrows(SQL4JsonExecutionException.class,
                    () -> apply("cast", input, targetType));
        }

        // --- CAST to INTEGER ---
        @Test
        void cast_numberToInteger() {
            var result = apply("cast", SqlNumber.of(2.7), new SqlString("INTEGER"));
            assertEquals(2.0, ((SqlNumber) result).doubleValue());
        }

        @Test
        void cast_negativeNumberToInteger() {
            var result = apply("cast", SqlNumber.of(-2.7), new SqlString("INTEGER"));
            assertEquals(-2.0, ((SqlNumber) result).doubleValue());
        }

        // --- CAST to BOOLEAN ---
        @Test
        void cast_stringToBoolean() {
            assertEquals(SqlBoolean.of(true), apply("cast", new SqlString("true"), new SqlString("BOOLEAN")));
        }

        @Test
        void cast_numberToBoolean() {
            assertEquals(SqlBoolean.of(true), apply("cast", SqlNumber.of(1), new SqlString("BOOLEAN")));
        }

        @Test
        void cast_zeroToBoolean() {
            assertEquals(SqlBoolean.of(false), apply("cast", SqlNumber.of(0), new SqlString("BOOLEAN")));
        }

        @Test
        void cast_dateToBoolean_throws() {
            SqlDate input = new SqlDate(LocalDate.now());
            SqlString targetType = new SqlString("BOOLEAN");
            assertThrows(SQL4JsonExecutionException.class,
                    () -> apply("cast", input, targetType));
        }

        // --- CAST to DATE ---
        @Test
        void cast_stringToDate() {
            assertEquals(new SqlDate(LocalDate.of(2024, 6, 15)),
                    apply("cast", new SqlString("2024-06-15"), new SqlString("DATE")));
        }

        @Test
        void cast_dateTimeToDate() {
            assertEquals(new SqlDate(LocalDate.of(2024, 6, 15)),
                    apply("cast", new SqlDateTime(LocalDateTime.of(2024, 6, 15, 10, 30)), new SqlString("DATE")));
        }

        @Test
        void cast_invalidStringToDate_throws() {
            SqlString input = new SqlString("not-a-date");
            SqlString targetType = new SqlString("DATE");
            assertThrows(SQL4JsonExecutionException.class,
                    () -> apply("cast", input, targetType));
        }

        // --- CAST null passthrough ---
        @Test
        void cast_nullToAnything_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("cast", SqlNull.INSTANCE, new SqlString("STRING")));
            assertEquals(SqlNull.INSTANCE, apply("cast", SqlNull.INSTANCE, new SqlString("NUMBER")));
        }

        @Test
        void cast_stringToString_identity() {
            var result = apply("cast", new SqlString("hello"), new SqlString("STRING"));
            assertEquals(new SqlString("hello"), result);
        }

        @Test
        void cast_numberToNumber_identity() {
            var result = apply("cast", SqlNumber.of(42), new SqlString("NUMBER"));
            assertEquals(SqlNumber.of(42), result);
        }

        @Test
        void cast_booleanToBoolean_identity() {
            assertEquals(SqlBoolean.TRUE, apply("cast", SqlBoolean.TRUE, new SqlString("BOOLEAN")));
        }

        @Test
        void cast_dateToDate_identity() {
            SqlDate d = new SqlDate(LocalDate.of(2024, 6, 15));
            assertEquals(d, apply("cast", d, new SqlString("DATE")));
        }

        @Test
        void cast_dateTimeToDateTime_identity() {
            SqlDateTime dt = new SqlDateTime(LocalDateTime.of(2024, 6, 15, 10, 0));
            assertEquals(dt, apply("cast", dt, new SqlString("DATETIME")));
        }

        @Test
        void cast_stringToDateTime() {
            var result = apply("cast", new SqlString("2024-06-15T10:30:00"), new SqlString("DATETIME"));
            assertInstanceOf(SqlDateTime.class, result);
            assertEquals(LocalDateTime.of(2024, 6, 15, 10, 30, 0), ((SqlDateTime) result).value());
        }

        @Test
        void cast_invalidStringToDateTime_throws() {
            SqlString input = new SqlString("not-a-datetime");
            SqlString targetType = new SqlString("DATETIME");
            assertThrows(SQL4JsonExecutionException.class,
                    () -> apply("cast", input, targetType));
        }

        @Test
        void cast_stringToInteger_wholeNumber() {
            var result = apply("cast", new SqlString("42"), new SqlString("INTEGER"));
            assertInstanceOf(SqlNumber.class, result);
            assertEquals(42.0, ((SqlNumber) result).doubleValue(), 1e-10);
        }

        @Test
        void cast_invalidStringToInteger_throws() {
            SqlString input = new SqlString("abc");
            SqlString targetType = new SqlString("INTEGER");
            assertThrows(SQL4JsonExecutionException.class,
                    () -> apply("cast", input, targetType));
        }

        @Test
        void cast_decimalAlias_sameAsNumber() {
            var result = apply("cast", new SqlString("42.5"), new SqlString("DECIMAL"));
            assertInstanceOf(SqlNumber.class, result);
            assertEquals(42.5, ((SqlNumber) result).doubleValue(), 1e-10);
        }

        @Test
        void cast_nullToString_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("cast", SqlNull.INSTANCE, new SqlString("STRING")));
        }

        @Test
        void cast_nullToNumber_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("cast", SqlNull.INSTANCE, new SqlString("NUMBER")));
        }

        @Test
        void cast_dateTimeToNumber_epochSeconds() {
            var dt = new SqlDateTime(LocalDateTime.of(2024, 1, 1, 0, 0));
            var result = apply("cast", dt, new SqlString("NUMBER"));
            assertInstanceOf(SqlNumber.class, result);
        }

        @Test
        void cast_dateToNumber_epochDay() {
            var d = new SqlDate(LocalDate.of(2024, 1, 1));
            var result = apply("cast", d, new SqlString("NUMBER"));
            assertInstanceOf(SqlNumber.class, result);
        }

        @Test
        void cast_numberToDateTime_throws() {
            SqlNumber input = SqlNumber.of(42);
            SqlString targetType = new SqlString("DATETIME");
            assertThrows(SQL4JsonExecutionException.class,
                    () -> apply("cast", input, targetType));
        }

        @Test
        void cast_dateTimeToInteger_throws() {
            SqlDateTime dt = new SqlDateTime(LocalDateTime.of(2024, 1, 1, 0, 0));
            SqlString targetType = new SqlString("INTEGER");
            assertThrows(SQL4JsonExecutionException.class,
                    () -> apply("cast", dt, targetType));
        }
    }

    @Nested
    class StringFunctionNullAndNonStringInputs {

        @Test
        void substring_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("substring", SqlNull.INSTANCE, SqlNumber.of(1), SqlNumber.of(3)));
        }

        @Test
        void substring_nonString_coerced() {
            // val coerced to "42"; substring("42", 1, 3) → "42"
            assertEquals(new SqlString("42"),
                    apply("substring", SqlNumber.of(42), SqlNumber.of(1), SqlNumber.of(3)));
        }

        @Test
        void substring_noArgs_defaultsFullString() {
            assertEquals(new SqlString("hello"), apply("substring", new SqlString("hello")));
        }

        @Test
        void substring_negativeStart_adjustsLength() {
            var result = apply("substring", new SqlString("hello"), SqlNumber.of(-1), SqlNumber.of(4));
            assertEquals(new SqlString("he"), result);
        }

        @Test
        void substring_negativeLength() {
            assertEquals(new SqlString(""), apply("substring", new SqlString("hello"), SqlNumber.of(1), SqlNumber.of(-1)));
        }

        @Test
        void trim_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("trim", SqlNull.INSTANCE));
        }

        @Test
        void trim_nonString_coerced() {
            assertEquals(new SqlString("42"), apply("trim", SqlNumber.of(42)));
        }

        @Test
        void length_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("length", SqlNull.INSTANCE));
        }

        @Test
        void length_nonString_coerced() {
            // "42".length() == 2
            assertEquals(SqlNumber.of(2), apply("length", SqlNumber.of(42)));
        }

        @Test
        void replace_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("replace", SqlNull.INSTANCE, new SqlString("a"), new SqlString("b")));
        }

        @Test
        void replace_nonString_coerced() {
            // "42".replace("4", "b") → "b2"
            assertEquals(new SqlString("b2"),
                    apply("replace", SqlNumber.of(42), new SqlString("4"), new SqlString("b")));
        }

        @Test
        void left_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("left", SqlNull.INSTANCE, SqlNumber.of(3)));
        }

        @Test
        void left_nonString_coerced() {
            // "42" with n=3 — capped at length, returns "42"
            assertEquals(new SqlString("42"), apply("left", SqlNumber.of(42), SqlNumber.of(3)));
        }

        @Test
        void right_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("right", SqlNull.INSTANCE, SqlNumber.of(3)));
        }

        @Test
        void right_nonString_coerced() {
            // "42" with n=3 — capped at length, returns "42"
            assertEquals(new SqlString("42"), apply("right", SqlNumber.of(42), SqlNumber.of(3)));
        }

        @Test
        void lpad_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("lpad", SqlNull.INSTANCE, SqlNumber.of(5), new SqlString("*")));
        }

        @Test
        void lpad_nonString_coerced() {
            // "42" padded to 5 with '*' → "***42"
            assertEquals(new SqlString("***42"),
                    apply("lpad", SqlNumber.of(42), SqlNumber.of(5), new SqlString("*")));
        }

        @Test
        void rpad_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("rpad", SqlNull.INSTANCE, SqlNumber.of(5), new SqlString("*")));
        }

        @Test
        void rpad_nonString_coerced() {
            // "42" padded to 5 with '*' → "42***"
            assertEquals(new SqlString("42***"),
                    apply("rpad", SqlNumber.of(42), SqlNumber.of(5), new SqlString("*")));
        }

        @Test
        void reverse_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("reverse", SqlNull.INSTANCE));
        }

        @Test
        void reverse_nonString_coerced() {
            // "42" reversed → "24"
            assertEquals(new SqlString("24"), apply("reverse", SqlNumber.of(42)));
        }

        @Test
        void position_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("position", SqlNull.INSTANCE, new SqlString("hello")));
        }

        @Test
        void position_nonString_coerced() {
            // POSITION(substr=42, str="hello") → "hello".indexOf("42") == -1 → 0
            assertEquals(SqlNumber.of(0), apply("position", SqlNumber.of(42), new SqlString("hello")));
        }

        @Test
        void lower_withLocaleArg() {
            var fn = registry.getScalar("lower").orElseThrow();
            SqlValue result = fn.apply().apply(new SqlString("HELLO"), List.of(new SqlString("en")));
            assertEquals(new SqlString("hello"), result);
        }

        @Test
        void upper_nonString_coerced() {
            // UPPER(42) → "42" (digits are unchanged)
            var fn = registry.getScalar("upper").orElseThrow();
            assertEquals(new SqlString("42"), fn.apply().apply(SqlNumber.of(42), List.of()));
        }

        @Test
        void lower_nonString_coerced() {
            // LOWER(42) → "42" (digits are unchanged)
            var fn = registry.getScalar("lower").orElseThrow();
            assertEquals(new SqlString("42"), fn.apply().apply(SqlNumber.of(42), List.of()));
        }

        @Test
        void lpad_padArgNonString_coerced() {
            // pad arg is numeric 0; coerced to "0" — pad by repeating "0"
            assertEquals(new SqlString("00042"),
                    apply("lpad", SqlNumber.of(42), SqlNumber.of(5), SqlNumber.of(0)));
        }

        @Test
        void replace_searchArgNonString_coerced() {
            // search arg is numeric 2; coerced to "2" — "h2llo".replace("2", "e")
            assertEquals(new SqlString("hello"),
                    apply("replace", new SqlString("h2llo"), SqlNumber.of(2), new SqlString("e")));
        }

        @Test
        void replace_replacementArgNonString_coerced() {
            // replacement arg numeric 0 → "0"
            assertEquals(new SqlString("h0llo"),
                    apply("replace", new SqlString("hello"), new SqlString("e"), SqlNumber.of(0)));
        }

        @Test
        void position_strArgNonString_coerced() {
            // POSITION("4", 142) → "142".indexOf("4") = 1 → 1-based = 2
            assertEquals(SqlNumber.of(2),
                    apply("position", new SqlString("4"), SqlNumber.of(142)));
        }

        @Test
        void upper_withLocaleArg() {
            var fn = registry.getScalar("upper").orElseThrow();
            SqlValue result = fn.apply().apply(new SqlString("hello"), List.of(new SqlString("en")));
            assertEquals(new SqlString("HELLO"), result);
        }
    }

    @Nested
    class MathFunctionNullAndNonNumberInputs {

        @Test
        void abs_nonNumber_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("abs", new SqlString("text")));
        }

        @Test
        void round_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("round", SqlNull.INSTANCE));
        }

        @Test
        void round_nonNumber_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("round", new SqlString("text")));
        }

        @Test
        void ceil_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("ceil", SqlNull.INSTANCE));
        }

        @Test
        void ceil_nonNumber_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("ceil", new SqlString("text")));
        }

        @Test
        void floor_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("floor", SqlNull.INSTANCE));
        }

        @Test
        void floor_nonNumber_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("floor", new SqlString("text")));
        }

        @Test
        void mod_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("mod", SqlNull.INSTANCE, SqlNumber.of(3)));
        }

        @Test
        void mod_nonNumber_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("mod", new SqlString("text"), SqlNumber.of(3)));
        }

        @Test
        void power_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("power", SqlNull.INSTANCE, SqlNumber.of(2)));
        }

        @Test
        void power_nonNumber_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("power", new SqlString("text"), SqlNumber.of(2)));
        }

        @Test
        void power_nan_returnsNull() {
            // negative base with fractional exponent produces NaN
            assertEquals(SqlNull.INSTANCE, apply("power", SqlNumber.of(-1), SqlNumber.of(0.5)));
        }

        @Test
        void sqrt_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("sqrt", SqlNull.INSTANCE));
        }

        @Test
        void sqrt_nonNumber_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("sqrt", new SqlString("text")));
        }

        @Test
        void sign_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("sign", SqlNull.INSTANCE));
        }

        @Test
        void sign_nonNumber_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("sign", new SqlString("text")));
        }
    }

    @Nested
    class DateFunctionEdgeCases {

        @Test
        void year_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("year", SqlNull.INSTANCE));
        }

        @Test
        void month_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("month", SqlNull.INSTANCE));
        }

        @Test
        void day_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("day", SqlNull.INSTANCE));
        }

        @Test
        void minute_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("minute", SqlNull.INSTANCE));
        }

        @Test
        void second_null_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("second", SqlNull.INSTANCE));
        }

        @Test
        void toDate_isoDateTimeString_returnsDateTime() {
            var result = apply("to_date", new SqlString("2024-06-15T10:30:00"));
            assertInstanceOf(SqlDateTime.class, result);
            assertEquals(LocalDateTime.of(2024, 6, 15, 10, 30, 0), ((SqlDateTime) result).value());
        }

        @Test
        void toDate_customFormat_parsesDateTime() {
            var result = apply("to_date", new SqlString("15/06/2024 10:30:00"),
                    new SqlString("dd/MM/yyyy HH:mm:ss"));
            assertInstanceOf(SqlDateTime.class, result);
        }

        @Test
        void dateAdd_dateTimeValue_staysDateTime() {
            var result = apply("date_add",
                    new SqlDateTime(LocalDateTime.of(2024, 6, 15, 10, 0)),
                    SqlNumber.of(5), new SqlString("DAY"));
            assertInstanceOf(SqlDateTime.class, result);
            assertEquals(LocalDateTime.of(2024, 6, 20, 10, 0), ((SqlDateTime) result).value());
        }

        @Test
        void dateAdd_nonDateValue_returnsNull() {
            assertEquals(SqlNull.INSTANCE,
                    apply("date_add", new SqlString("not-a-date"), SqlNumber.of(1), new SqlString("DAY")));
        }

        @Test
        void dateDiff_nonDateInputs_returnsNull() {
            assertEquals(SqlNull.INSTANCE,
                    apply("date_diff", new SqlString("not-a-date"),
                            new SqlDate(LocalDate.of(2024, 1, 1)), new SqlString("DAY")));
        }

        @Test
        void year_nonDateString_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("year", new SqlString("not-a-date")));
        }

        @Test
        void hour_nonDateTimeString_returnsNull() {
            assertEquals(SqlNull.INSTANCE, apply("hour", new SqlString("not-a-datetime")));
        }
    }

    @Nested
    class ValueFunctionLookup {

        @Test
        void lookup_unknownValue_returnsEmpty() {
            assertTrue(registry.getValue("nonexistent").isEmpty());
        }
    }
}
