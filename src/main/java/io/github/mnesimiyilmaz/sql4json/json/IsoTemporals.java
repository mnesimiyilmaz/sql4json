// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.json;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

/**
 * Internal ISO temporal parsing helpers shared across the library. The {@code json} package is not exported from the
 * JPMS module, so this class is only reachable from sibling internal packages ({@code mapper}, {@code registry}).
 *
 * <p>All methods are null-safe on failure — they return {@code null} rather than throwing, so callers can chain
 * fallbacks.
 */
public final class IsoTemporals {

    private IsoTemporals() {}

    /**
     * Parse an ISO date or ISO datetime string, returning the date portion.
     *
     * @param s candidate string
     * @return a {@link LocalDate} on success, or {@code null} if neither format matches
     */
    public static LocalDate tryParseDate(String s) {
        if (s == null) return null;
        try {
            return LocalDate.parse(s, DateTimeFormatter.ISO_DATE);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            TemporalAccessor ta = DateTimeFormatter.ISO_DATE_TIME.parse(s);
            return LocalDate.from(ta);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        return null;
    }

    /**
     * Parse an ISO datetime string, or an ISO date string (interpreted at start of day).
     *
     * @param s candidate string
     * @return a {@link LocalDateTime} on success, or {@code null} if neither format matches
     */
    public static LocalDateTime tryParseDateTime(String s) {
        if (s == null) return null;
        try {
            TemporalAccessor ta = DateTimeFormatter.ISO_DATE_TIME.parse(s);
            return LocalDateTime.of(LocalDate.from(ta), LocalTime.from(ta));
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            return LocalDate.parse(s, DateTimeFormatter.ISO_DATE).atStartOfDay();
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        return null;
    }

    /**
     * Parse an ISO instant string (UTC or offset datetime).
     *
     * @param s candidate string
     * @return an {@link Instant} on success, or {@code null} if not parseable
     */
    public static Instant tryParseInstant(String s) {
        if (s == null) return null;
        try {
            return Instant.parse(s);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }
}
