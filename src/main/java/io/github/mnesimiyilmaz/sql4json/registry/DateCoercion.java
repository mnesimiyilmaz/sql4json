package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.types.SqlDate;
import io.github.mnesimiyilmaz.sql4json.types.SqlDateTime;
import io.github.mnesimiyilmaz.sql4json.types.SqlString;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

final class DateCoercion {

    private DateCoercion() {
    }

    /**
     * Coerces a {@link SqlValue} to a {@link LocalDate}.
     * Handles {@link SqlDate}, {@link SqlDateTime}, and ISO date/datetime {@link SqlString} values.
     *
     * @param val the SQL value to coerce
     * @return a {@link LocalDate}, or {@code null} if coercion is not possible
     */
    static LocalDate toLocalDate(SqlValue val) {
        return switch (val) {
            case SqlDate(var v) -> v;
            case SqlDateTime(var v) -> v.toLocalDate();
            case SqlString(var s) -> tryParseDate(s);
            default -> null;
        };
    }

    /**
     * Coerces a {@link SqlValue} to a {@link LocalDateTime}.
     * Handles {@link SqlDateTime}, {@link SqlDate}, and ISO date/datetime {@link SqlString} values.
     *
     * @param val the SQL value to coerce
     * @return a {@link LocalDateTime}, or {@code null} if coercion is not possible
     */
    static LocalDateTime toLocalDateTime(SqlValue val) {
        return switch (val) {
            case SqlDateTime(var v) -> v;
            case SqlDate(var v) -> v.atStartOfDay();
            case SqlString(var s) -> tryParseDateTime(s);
            default -> null;
        };
    }

    private static LocalDateTime tryParseDateTime(String s) {
        try {
            TemporalAccessor ta = DateTimeFormatter.ISO_DATE_TIME.parse(s);
            return LocalDateTime.of(LocalDate.from(ta), LocalTime.from(ta));
        } catch (DateTimeParseException ignored) {
            // not an ISO datetime — fall through
        }
        try {
            return LocalDate.parse(s, DateTimeFormatter.ISO_DATE).atStartOfDay();
        } catch (DateTimeParseException ignored) {
            // not an ISO date either
        }
        return null;
    }

    private static LocalDate tryParseDate(String s) {
        try {
            return LocalDate.parse(s, DateTimeFormatter.ISO_DATE);
        } catch (DateTimeParseException ignored) {
            // not an ISO date — fall through
        }
        try {
            TemporalAccessor ta = DateTimeFormatter.ISO_DATE_TIME.parse(s);
            return LocalDate.from(ta);
        } catch (DateTimeParseException ignored) {
            // not an ISO datetime either
        }
        return null;
    }
}
