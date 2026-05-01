package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.FlatRow;
import io.github.mnesimiyilmaz.sql4json.engine.RowSchema;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.json.JsonArrayValue;
import io.github.mnesimiyilmaz.sql4json.json.JsonFlattener;
import io.github.mnesimiyilmaz.sql4json.json.JsonObjectValue;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.*;

/**
 * Fluent builder for {@link SQL4JsonEngine}.
 *
 * <p>Obtain an instance via {@link SQL4Json#engine()}. At least one data source
 * (unnamed via {@link #data(String)} or named via {@link #data(String, String)})
 * must be configured before {@link #build()}. Unnamed and named sources can coexist.</p>
 *
 * <p>Call {@link #settings(Sql4jsonSettings)} to supply codec, cache, security, and
 * limits configuration. Settings may be applied in any order relative to
 * {@link #data(String)} — JSON parsing is deferred to {@link #build()}, so a late
 * {@code .settings(custom)} call is never silently ignored.</p>
 *
 * <p>Usage example:</p>
 * <pre>{@code
 * SQL4JsonEngine engine = SQL4Json.engine()
 *     .settings(Sql4jsonSettings.builder()
 *         .cache(c -> c.queryResultCacheEnabled(true))
 *         .limits(l -> l.maxRowsPerQuery(500_000))
 *         .build())
 *     .data(jsonString)
 *     .build();
 * String result = engine.query("SELECT * FROM $r WHERE active = true");
 * }</pre>
 */
public final class SQL4JsonEngineBuilder {

    private Sql4jsonSettings       settings = Sql4jsonSettings.defaults();
    private String                 rawJson;             // from data(String)
    private JsonValue              directData;          // from data(JsonValue)
    private Map<String, String>    rawNamedSources;     // from data(String, String)
    private Map<String, JsonValue> directNamedSources;  // from data(String, JsonValue)

    SQL4JsonEngineBuilder() {
    }

    /**
     * Apply a {@link Sql4jsonSettings} to this engine. Settings control the JSON codec,
     * query-result cache, security policy, and numeric limits. May be called before or after
     * {@link #data(String)} — parsing is deferred to {@link #build()}.
     *
     * @param settings non-null settings instance
     * @return this builder
     * @throws NullPointerException if {@code settings} is null
     */
    public SQL4JsonEngineBuilder settings(Sql4jsonSettings settings) {
        this.settings = Objects.requireNonNull(settings, "settings");
        return this;
    }

    /**
     * Bind a JSON string as the unnamed (root) data source, referenced as {@code $r} in queries.
     * Parsing is deferred to {@link #build()} so that a later {@link #settings(Sql4jsonSettings)}
     * call with a custom codec is always honoured.
     *
     * @param jsonString JSON string to bind as the root data source
     * @return this builder
     * @throws SQL4JsonException if {@code jsonString} is null
     */
    public SQL4JsonEngineBuilder data(String jsonString) {
        if (jsonString == null) {
            throw new SQL4JsonException("Input data must not be null");
        }
        this.rawJson = jsonString;
        this.directData = null;
        return this;
    }

    /**
     * Bind a {@link JsonValue} as the unnamed (root) data source, referenced as {@code $r} in queries.
     *
     * @param jsonValue pre-parsed JSON data to bind as the root data source
     * @return this builder
     * @throws SQL4JsonException if {@code jsonValue} is null
     */
    public SQL4JsonEngineBuilder data(JsonValue jsonValue) {
        if (jsonValue == null) {
            throw new SQL4JsonException("Input data must not be null");
        }
        this.directData = jsonValue;
        this.rawJson = null;
        return this;
    }

    /**
     * Bind a named JSON source (String) for use in JOIN queries. The name becomes the
     * table identifier in {@code FROM}/{@code JOIN} clauses.
     * Parsing is deferred to {@link #build()} so that a later {@link #settings(Sql4jsonSettings)}
     * call with a custom codec is always honoured.
     *
     * @param name       table identifier used in FROM/JOIN clauses
     * @param jsonString JSON string for this named source
     * @return this builder
     * @throws SQL4JsonException if {@code name} or {@code jsonString} is null or blank
     */
    public SQL4JsonEngineBuilder data(String name, String jsonString) {
        validateNamedSource(name, jsonString);
        if (rawNamedSources == null) rawNamedSources = new LinkedHashMap<>();
        rawNamedSources.put(name, jsonString);
        return this;
    }

    /**
     * Bind a named JSON source ({@link JsonValue}) for use in JOIN queries. The name becomes
     * the table identifier in {@code FROM}/{@code JOIN} clauses.
     *
     * @param name      table identifier used in FROM/JOIN clauses
     * @param jsonValue pre-parsed JSON data for this named source
     * @return this builder
     * @throws SQL4JsonException if {@code name} or {@code jsonValue} is null (or name is blank)
     */
    public SQL4JsonEngineBuilder data(String name, JsonValue jsonValue) {
        validateNamedSource(name, jsonValue);
        if (directNamedSources == null) directNamedSources = new LinkedHashMap<>();
        directNamedSources.put(name, jsonValue);
        return this;
    }

    private static void validateNamedSource(String name, Object data) {
        if (name == null || name.isBlank()) {
            throw new SQL4JsonException("Source name must not be null or blank");
        }
        if (data == null) {
            throw new SQL4JsonException("Input data must not be null");
        }
    }

    /**
     * Build the configured {@link SQL4JsonEngine}. At least one data source must be bound.
     * All JSON strings are parsed here using the codec from the bound {@link Sql4jsonSettings}.
     * JSON-codec limits from {@link Sql4jsonSettings} (e.g. maximum input length and nesting
     * depth configured via {@code DefaultJsonCodecSettings}) are enforced at this point for
     * any raw JSON strings supplied via {@link #data(String)} or {@link #data(String, String)}.
     *
     * @return the configured engine
     * @throws SQL4JsonException                                                     if no data source has been configured
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if a JSON
     *                                                                               string fails to parse or a configured JSON codec limit from
     *                                                                               {@link Sql4jsonSettings} is exceeded
     */
    public SQL4JsonEngine build() {
        boolean hasUnnamed = rawJson != null || directData != null;
        boolean hasNamed = (rawNamedSources != null && !rawNamedSources.isEmpty())
                || (directNamedSources != null && !directNamedSources.isEmpty());
        if (!hasUnnamed && !hasNamed) {
            throw new SQL4JsonException("Data must be set before building engine");
        }

        JsonCodec codec = settings.codec();
        JsonValue resolvedData = resolvePrimaryData(codec);
        Map<String, JsonValue> resolvedNamedSources = hasNamed ? resolveNamedSources(codec) : null;

        List<FlatRow> preFlattenedRows = resolvedData != null ? preFlattenRows(resolvedData) : List.of();
        QueryResultCache resolvedCache = resolveCache();

        if (!preFlattenedRows.isEmpty() && rawJson != null) {
            return new SQL4JsonEngine(rawJson, null, preFlattenedRows, resolvedCache,
                    codec, resolvedNamedSources, settings);
        }
        return new SQL4JsonEngine(null, resolvedData, preFlattenedRows, resolvedCache,
                codec, resolvedNamedSources, settings);
    }

    private JsonValue resolvePrimaryData(JsonCodec codec) {
        if (directData != null) return directData;
        if (rawJson != null) return codec.parse(rawJson);
        return null;
    }

    private Map<String, JsonValue> resolveNamedSources(JsonCodec codec) {
        Map<String, JsonValue> merged = new LinkedHashMap<>();
        if (rawNamedSources != null) {
            rawNamedSources.forEach((n, j) -> merged.put(n, codec.parse(j)));
        }
        if (directNamedSources != null) {
            merged.putAll(directNamedSources);
        }
        return Collections.unmodifiableMap(merged);
    }

    private QueryResultCache resolveCache() {
        var cfg = settings.cache();
        if (cfg.customCache() != null) return cfg.customCache();
        if (cfg.queryResultCacheEnabled()) return new LruQueryResultCache(cfg.queryResultCacheSize());
        return null;
    }

    /**
     * Pre-flatten all elements of a root JSON array into fully-resolved {@link FlatRow}s.
     * Returns an empty list if data is not an array (single object, primitive, etc.).
     * The returned rows share a single {@link RowSchema} (column union across all rows)
     * and are immutable and safe to share across concurrent queries.
     */
    private static List<FlatRow> preFlattenRows(JsonValue data) {
        if (!(data instanceof JsonArrayValue(var elements))) {
            return List.of();
        }
        FieldKey.Interner interner = new FieldKey.Interner();
        // Two-pass: first collect schema across all rows, then materialize each row.
        var schemaBuilder = new RowSchema.Builder();
        var rowMaps = new ArrayList<Map<FieldKey, SqlValue>>(elements.size());
        for (JsonValue element : elements) {
            Map<FieldKey, SqlValue> fields = HashMap.newHashMap(estimateFlatSize(element));
            JsonFlattener.flattenInto(element, fields, interner);
            fields.keySet().forEach(schemaBuilder::add);
            rowMaps.add(fields);
        }
        RowSchema schema = schemaBuilder.build();
        var rows = new ArrayList<FlatRow>(rowMaps.size());
        for (var fields : rowMaps) {
            Object[] vals = new Object[schema.size()];
            for (int i = 0; i < schema.size(); i++) {
                FieldKey k = schema.columnAt(i);
                SqlValue v = fields.get(k);
                if (v != null && !(v instanceof SqlNull)) {
                    vals[i] = v;
                }
            }
            rows.add(FlatRow.preFlattened(schema, vals));
        }
        return Collections.unmodifiableList(rows);
    }

    /**
     * Hint for {@link HashMap#newHashMap(int)} — keeps the per-row hash table
     * from resizing 3-4 times during full flatten on typical 13-field rows.
     * Falls back to a small default for non-object inputs (rare).
     */
    @io.github.mnesimiyilmaz.sql4json.internal.SkipCoverageGenerated
    private static int estimateFlatSize(JsonValue v) {
        if (v instanceof JsonObjectValue(var fields)) {
            return Math.max(8, fields.size() + (fields.size() >>> 1));
        }
        return 8;
    }
}
