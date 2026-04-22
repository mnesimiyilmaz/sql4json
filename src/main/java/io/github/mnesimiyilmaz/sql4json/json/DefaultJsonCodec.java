package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.settings.DefaultJsonCodecSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.Objects;

/**
 * Built-in {@link JsonCodec} backed by the library's own JSON parser and serializer.
 * This is the default used when no custom codec is provided.
 */
@SuppressWarnings("ClassCanBeRecord")
public final class DefaultJsonCodec implements JsonCodec {

    private final DefaultJsonCodecSettings settings;

    /**
     * Creates a codec with the default safe limits.
     */
    public DefaultJsonCodec() {
        this(DefaultJsonCodecSettings.defaults());
    }

    /**
     * Creates a {@code DefaultJsonCodec} with the supplied settings.
     *
     * <p>Use this constructor when the default security limits need to be adjusted —
     * for example, to accept larger inputs or to change the {@link DefaultJsonCodecSettings#duplicateKeyPolicy()}.
     * The supplied settings are applied by both {@code JsonParser} and
     * {@code StreamingJsonParser} during every parse operation.
     *
     * <p>Usage example:
     * <pre>{@code
     * DefaultJsonCodecSettings codecSettings = DefaultJsonCodecSettings.builder()
     *     .maxInputLength(50 * 1024 * 1024)
     *     .build();
     *
     * Sql4jsonSettings settings = Sql4jsonSettings.builder()
     *     .codec(new DefaultJsonCodec(codecSettings))
     *     .build();
     * }</pre>
     *
     * @param settings non-null codec settings; use {@link DefaultJsonCodecSettings#defaults()}
     *                 for the default safe limits
     * @throws NullPointerException if {@code settings} is {@code null}
     * @see DefaultJsonCodecSettings
     */
    public DefaultJsonCodec(DefaultJsonCodecSettings settings) {
        this.settings = Objects.requireNonNull(settings, "settings");
    }

    /**
     * Returns the codec settings used by this instance.
     *
     * @return the codec settings
     */
    public DefaultJsonCodecSettings settings() {
        return settings;
    }

    @Override
    public JsonValue parse(String json) {
        return JsonParser.parse(json, settings);
    }

    @Override
    public String serialize(JsonValue value) {
        return JsonSerializer.serialize(value);
    }
}
