// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.settings;

import static org.junit.jupiter.api.Assertions.*;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import org.junit.jupiter.api.Test;

class Sql4jsonSettingsBuilderTests {

    // ── Builder defaults and composition ─────────────────────────────────

    @Test
    void defaults_compose_subsection_defaults() {
        var s = Sql4jsonSettings.defaults();
        assertSame(SecuritySettings.defaults(), s.security());
        assertSame(LimitsSettings.defaults(), s.limits());
        assertSame(CacheSettings.defaults(), s.cache());
        assertTrue(s.codec() instanceof DefaultJsonCodec);
    }

    @Test
    void builder_consumer_overrides_one_subsection_only() {
        var s = Sql4jsonSettings.builder().limits(l -> l.maxSqlLength(1024)).build();
        assertEquals(1024, s.limits().maxSqlLength());
        assertEquals(16, s.security().maxLikeWildcards());
    }

    @Test
    void builder_overrides_multiple_subsections() {
        var s = Sql4jsonSettings.builder()
                .security(sec -> sec.maxLikeWildcards(4).redactErrorDetails(false))
                .limits(l -> l.maxSubqueryDepth(4))
                .cache(c -> c.likePatternCacheSize(32))
                .build();
        assertEquals(4, s.security().maxLikeWildcards());
        assertFalse(s.security().redactErrorDetails());
        assertEquals(4, s.limits().maxSubqueryDepth());
        assertEquals(32, s.cache().likePatternCacheSize());
    }

    @Test
    void builder_accepts_custom_codec_instance() {
        JsonCodec fake = new JsonCodec() {
            public io.github.mnesimiyilmaz.sql4json.types.JsonValue parse(String s) {
                return null;
            }

            public String serialize(io.github.mnesimiyilmaz.sql4json.types.JsonValue v) {
                return "";
            }
        };
        var s = Sql4jsonSettings.builder().codec(fake).build();
        assertSame(fake, s.codec());
    }

    @Test
    void builder_rejects_null_codec() {
        assertThrows(
                NullPointerException.class,
                () -> Sql4jsonSettings.builder().codec((JsonCodec) null).build());
    }

    @Test
    void settings_is_immutable_record() {
        var a = Sql4jsonSettings.defaults();
        var b = Sql4jsonSettings.defaults();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void builder_rejects_zero_queryResultCacheSize() {
        assertThrows(
                IllegalArgumentException.class, () -> CacheSettings.builder().queryResultCacheSize(0));
    }

    @Test
    void builder_rejects_negative_queryResultCacheSize() {
        assertThrows(
                IllegalArgumentException.class, () -> CacheSettings.builder().queryResultCacheSize(-1));
    }

    // ── Security settings ───────────────────────────────────────────────

    @Test
    void maxLikeWildcards_rejects_oversized_literal_like_pattern() {
        var settings =
                Sql4jsonSettings.builder().security(s -> s.maxLikeWildcards(3)).build();
        String sql = "SELECT * FROM $r WHERE name LIKE '%a%b%c%d%'"; // 5 wildcards
        var ex = assertThrows(SQL4JsonException.class, () -> QueryParser.parse(sql, settings));
        assertTrue(ex.getMessage().contains("LIKE pattern wildcard count exceeds configured maximum"));
    }

    @Test
    void maxLikeWildcards_rejects_oversized_literal_not_like_pattern() {
        var settings =
                Sql4jsonSettings.builder().security(s -> s.maxLikeWildcards(3)).build();
        String sql = "SELECT * FROM $r WHERE name NOT LIKE '%a%b%c%d%'";
        var ex = assertThrows(SQL4JsonException.class, () -> QueryParser.parse(sql, settings));
        assertTrue(ex.getMessage().contains("LIKE pattern wildcard count exceeds configured maximum"));
    }

    @Test
    void maxLikeWildcards_accepts_pattern_at_exact_limit() {
        var settings =
                Sql4jsonSettings.builder().security(s -> s.maxLikeWildcards(3)).build();
        String sql = "SELECT * FROM $r WHERE name LIKE '%a%b%'"; // exactly 3
        assertDoesNotThrow(() -> QueryParser.parse(sql, settings));
    }
}
