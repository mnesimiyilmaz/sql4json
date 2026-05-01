package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import io.github.mnesimiyilmaz.sql4json.types.*;

import java.math.BigInteger;
import java.time.*;

/**
 * Converts a Java {@link Object} parameter value into an internal {@link SqlValue} suitable
 * for substitution into a {@code ParameterRef}. Supports the common Java types users will
 * hand to {@code PreparedQuery.execute(...)} and all the SqlValue subtypes themselves (for
 * passthrough when the caller binds a pre-built {@code SqlValue}).
 *
 * <p>Supported types:
 * <ul>
 *   <li>{@link String}</li>
 *   <li>All primitive numeric wrappers: {@code Byte, Short, Integer, Long, Float, Double}</li>
 *   <li>{@link java.math.BigDecimal}, {@link java.math.BigInteger}, other {@link Number}</li>
 *   <li>{@link Boolean}</li>
 *   <li>{@link LocalDate}, {@link LocalDateTime}</li>
 *   <li>{@link Instant}, {@link ZonedDateTime}, {@link OffsetDateTime} — normalised to UTC
 *       local-datetime</li>
 *   <li>{@link java.util.Date}</li>
 *   <li>Already-wrapped {@link SqlValue} — returned as is</li>
 *   <li>{@code null} → {@link SqlNull#INSTANCE}</li>
 * </ul>
 *
 * <p>Unsupported types throw {@link SQL4JsonBindException}.
 */
public final class ParameterConverter {

    private ParameterConverter() {
        // Utility class — not instantiable
    }

    /**
     * Converts an arbitrary Java value to an {@link SqlValue}.
     *
     * @param value any Java value (may be {@code null})
     * @return the equivalent {@code SqlValue}
     * @throws SQL4JsonBindException if the value's runtime class has no supported conversion
     */
    public static SqlValue toSqlValue(Object value) {
        return switch (value) {
            case null -> SqlNull.INSTANCE;
            case SqlValue sv -> sv;
            case String s -> new SqlString(s);
            case Boolean b -> SqlBoolean.of(b);
            case Byte n -> SqlNumber.of(n.longValue());
            case Short n -> SqlNumber.of(n.longValue());
            case Integer n -> SqlNumber.of(n.longValue());
            case Long n -> SqlNumber.of(n.longValue());
            case Float n -> SqlNumber.of(n.doubleValue());
            case Double n -> SqlNumber.of(n.doubleValue());
            case BigInteger bi -> convertBigInteger(bi);
            case Number n -> SqlNumber.of(n.doubleValue());
            case LocalDate d -> new SqlDate(d);
            case LocalDateTime dt -> new SqlDateTime(dt);
            case Instant i -> new SqlDateTime(LocalDateTime.ofInstant(i, ZoneOffset.UTC));
            case ZonedDateTime zdt -> new SqlDateTime(zdt.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime());
            case OffsetDateTime odt -> new SqlDateTime(odt.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
            case java.util.Date d -> new SqlDateTime(LocalDateTime.ofInstant(d.toInstant(), ZoneOffset.UTC));
            default -> throw new SQL4JsonBindException(
                    "Unsupported parameter type: " + value.getClass().getName());
        };
    }

    private static SqlNumber convertBigInteger(BigInteger bi) {
        // Prefer long path for representable values — preserves exact int semantics in
        // comparisons. Oversized BigInteger is preserved as BigDecimal for exact precision.
        try {
            return SqlNumber.of(bi.longValueExact());
        } catch (ArithmeticException ex) {
            return SqlNumber.of(new java.math.BigDecimal(bi));
        }
    }
}
