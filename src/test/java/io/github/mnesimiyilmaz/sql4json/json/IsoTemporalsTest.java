package io.github.mnesimiyilmaz.sql4json.json;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class IsoTemporalsTest {

    @Test
    void when_valid_iso_date_then_parse_returns_local_date() {
        assertEquals(LocalDate.of(2026, 4, 21), IsoTemporals.tryParseDate("2026-04-21"));
    }

    @Test
    void when_iso_datetime_passed_then_parse_date_extracts_date_portion() {
        assertEquals(LocalDate.of(2026, 4, 21), IsoTemporals.tryParseDate("2026-04-21T10:15:30"));
    }

    @Test
    void when_non_iso_string_then_parse_date_returns_null() {
        assertNull(IsoTemporals.tryParseDate("21/04/2026"));
        assertNull(IsoTemporals.tryParseDate("not a date"));
    }

    @Test
    void when_valid_iso_datetime_then_parse_returns_local_datetime() {
        assertEquals(LocalDateTime.of(2026, 4, 21, 10, 15, 30),
                IsoTemporals.tryParseDateTime("2026-04-21T10:15:30"));
    }

    @Test
    void when_iso_date_only_passed_then_parse_datetime_returns_start_of_day() {
        assertEquals(LocalDateTime.of(2026, 4, 21, 0, 0),
                IsoTemporals.tryParseDateTime("2026-04-21"));
    }

    @Test
    void when_valid_iso_instant_then_parse_returns_instant() {
        Instant expected = Instant.parse("2026-04-21T10:15:30Z");
        assertEquals(expected, IsoTemporals.tryParseInstant("2026-04-21T10:15:30Z"));
    }

    @Test
    void when_non_iso_then_parse_instant_returns_null() {
        assertNull(IsoTemporals.tryParseInstant("not an instant"));
    }

    @Test
    void when_null_input_then_all_methods_return_null() {
        assertNull(IsoTemporals.tryParseDate(null));
        assertNull(IsoTemporals.tryParseDateTime(null));
        assertNull(IsoTemporals.tryParseInstant(null));
    }
}
