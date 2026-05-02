// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.settings.DefaultJsonCodecSettings;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;

/**
 * Shared {@link Sql4jsonSettings} constant for {@code @Tag("large")} tests.
 *
 * <p>The default {@code DefaultJsonCodecSettings} caps JSON input at 10 MiB and arrays at 1,000,000 elements.
 * Large-dataset tests intentionally exceed those limits, so they must pass an explicit settings instance with raised
 * limits.
 *
 * <ul>
 *   <li>{@code maxInputLength} — {@link Integer#MAX_VALUE} (~2.1 GB): accommodates the 512 MB profiling file (loaded as
 *       a Java {@code String}, whose length fits in an {@code int}) and the ~24 MB generated wide-array fixtures.
 *   <li>{@code maxArrayElements} — {@code 10_000_000}: accommodates the 512 MB file whose element count can exceed the
 *       1 M default.
 *   <li>{@code maxStringLength} — {@code 10 * 1024 * 1024} (10 MiB): bio / string fields in the 512 MB fixture are
 *       short, but 10 MiB gives generous headroom.
 *   <li>{@code maxRowsPerQuery} — {@code 10_000_000}: covers full-scan queries over the 512 MB file without hitting the
 *       1 M row-count guard.
 * </ul>
 */
final class LargeTestSettings {

    static final Sql4jsonSettings INSTANCE = Sql4jsonSettings.builder()
            .codec(new DefaultJsonCodec(DefaultJsonCodecSettings.builder()
                    .maxInputLength(Integer.MAX_VALUE)
                    .maxArrayElements(10_000_000)
                    .maxStringLength(10 * 1024 * 1024)
                    .build()))
            .limits(l -> l.maxRowsPerQuery(10_000_000))
            .build();

    private LargeTestSettings() {}
}
