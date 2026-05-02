// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.engine.FlatRow;
import io.github.mnesimiyilmaz.sql4json.engine.ParameterSubstitutor;
import io.github.mnesimiyilmaz.sql4json.engine.QueryExecutor;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import java.util.List;
import java.util.Map;

/**
 * Stateful query engine with bound JSON data, an optional result cache, and bound {@link Sql4jsonSettings} that governs
 * all parsing and execution within this instance.
 *
 * <p>Designed to be long-lived (e.g., one per dataset). Data is bound once at build time and reused across queries.
 * When an {@link QueryResultCache} is configured (via {@link Sql4jsonSettings#cache()}), deterministic query results
 * are cached; non-deterministic queries (e.g., those using {@code NOW()}) bypass the cache.
 *
 * <p>Thread-safe: all fields are immutable after construction, and the underlying executor creates query-scoped state
 * on each call. Cache implementations must be thread-safe.
 *
 * <p>Use {@link SQL4Json#engine()} to obtain a {@link SQL4JsonEngineBuilder}.
 *
 * <p>Usage example:
 *
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
public final class SQL4JsonEngine {

    private static final QueryExecutor EXECUTOR = new QueryExecutor();

    private final String rawJson; // non-null when tree was released (string-based)
    private final JsonValue data; // non-null when tree is kept (JsonValue-based)
    private final List<FlatRow> preFlattenedRows; // empty if data is not a top-level array
    private final QueryResultCache cache; // null if caching not configured
    private final JsonCodec codec;
    private final Map<String, JsonValue> namedSources; // null if no named sources configured
    private final Sql4jsonSettings settings;

    SQL4JsonEngine(
            String rawJson,
            JsonValue data,
            List<FlatRow> preFlattenedRows,
            QueryResultCache cache,
            JsonCodec codec,
            Map<String, JsonValue> namedSources,
            Sql4jsonSettings settings) {
        this.rawJson = rawJson;
        this.data = data;
        this.preFlattenedRows = preFlattenedRows;
        this.cache = cache;
        this.codec = codec;
        this.namedSources = namedSources;
        this.settings = settings;
    }

    /**
     * Execute a SQL query against the bound data, returning the result as a JSON string. Parsing and execution limits
     * are governed by the {@link Sql4jsonSettings} bound at build time.
     *
     * @param sql SQL SELECT query
     * @return result serialized as a JSON string
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException if the SQL has syntax errors or exceeds
     *     the configured SQL length limit (see {@link Sql4jsonSettings})
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if an error occurs during
     *     execution, including when a configured limit from {@link Sql4jsonSettings} is exceeded (IN list size, LIKE
     *     wildcard count, subquery depth, materialized row count, or JSON codec limits when re-parsing is required)
     */
    public String query(String sql) {
        return codec.serialize(queryAsJsonValue(sql));
    }

    /**
     * Execute a SQL query against the bound data, returning the result as a {@link JsonValue}. Parsing and execution
     * limits are governed by the {@link Sql4jsonSettings} bound at build time.
     *
     * @param sql SQL SELECT query
     * @return result as a {@code JsonValue}
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException if the SQL has syntax errors or exceeds
     *     the configured SQL length limit (see {@link Sql4jsonSettings})
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if an error occurs during
     *     execution, including when a configured limit from {@link Sql4jsonSettings} is exceeded (IN list size, LIKE
     *     wildcard count, subquery depth, materialized row count, or JSON codec limits when re-parsing is required)
     */
    public JsonValue queryAsJsonValue(String sql) {
        SQL4Json.validateQuery(sql);

        if (cache != null) {
            JsonValue cached = cache.get(sql);
            if (cached != null) {
                return cached;
            }

            QueryDefinition definition = QueryParser.parse(sql, settings);
            JsonValue result = executeQuery(definition);
            if (!definition.containsNonDeterministic()) {
                cache.put(sql, result);
            }
            return result;
        }

        return executeQuery(QueryParser.parse(sql, settings));
    }

    private JsonValue executeQuery(QueryDefinition definition) {
        if (definition.joins() != null && !definition.joins().isEmpty()) {
            if (namedSources == null || namedSources.isEmpty()) {
                throw new io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException(
                        "JOIN queries require named data sources. Use engine().data(name, json) to bind sources.");
            }
            return EXECUTOR.execute(definition, namedSources, settings);
        }
        return EXECUTOR.execute(definition, resolveData(definition), preFlattenedRows, settings);
    }

    /**
     * Resolve data for query execution. When the tree was released (string-based Engine), re-parse from raw JSON only
     * if the query actually needs the tree (subqueries or non-root FROM). For normal queries with pre-flattened rows,
     * returns null.
     */
    private JsonValue resolveData(QueryDefinition definition) {
        if (data != null) return data;
        // Tree was released — only re-parse if the query needs the original tree
        if (definition.fromSubQuery() != null
                || (definition.rootPath() != null && !definition.rootPath().equals("$r"))) {
            return codec.parse(rawJson);
        }
        return null;
    }

    /**
     * Execute a query against bound data and map the single-row result to {@code type}. Unwrap rules match
     * {@link SQL4Json#queryAs(String, String, Class)}.
     *
     * @param sql SQL SELECT query
     * @param type target class
     * @param <T> target type
     * @return mapped instance
     * @since 1.1.0
     */
    public <T> T queryAs(String sql, Class<T> type) {
        JsonValue raw = queryAsJsonValue(sql);
        return SQL4Json.unwrapAndMap(raw, type, settings);
    }

    /**
     * Execute a query against bound data and map each row to {@code type}.
     *
     * @param sql SQL SELECT query
     * @param type target class
     * @param <T> target type
     * @return unmodifiable list of mapped instances
     * @since 1.1.0
     */
    public <T> List<T> queryAsList(String sql, Class<T> type) {
        JsonValue raw = queryAsJsonValue(sql);
        return SQL4Json.mapList(raw, type, settings);
    }

    /**
     * Executes a parameterised SQL query against the bound data. Parameterised queries always bypass the result cache:
     * parse reuse is already delivered by {@link PreparedQuery}, and result caching across distinct bind sets is
     * low-value given the near-unbounded key space.
     *
     * @param sql SQL SELECT query with {@code ?} / {@code :name} placeholders
     * @param params bound parameter values
     * @return result serialised as a JSON string
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException if binding fails
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException if SQL is invalid
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException on exec errors
     * @since 1.1.0
     */
    public String query(String sql, BoundParameters params) {
        return codec.serialize(queryAsJsonValue(sql, params));
    }

    /**
     * Executes a parameterised SQL query and returns the result as a {@link JsonValue}. See {@link #query(String,
     * BoundParameters)} for caching semantics.
     *
     * @param sql SQL SELECT query with placeholders
     * @param params bound parameter values
     * @return result as a {@code JsonValue}
     * @since 1.1.0
     */
    public JsonValue queryAsJsonValue(String sql, BoundParameters params) {
        SQL4Json.validateQuery(sql);
        QueryDefinition definition = QueryParser.parse(sql, settings);
        boolean hasParams = definition.positionalCount() > 0
                || !definition.namedParameters().isEmpty()
                || definition.limitParam() != null
                || definition.offsetParam() != null;
        if (!hasParams) {
            // No placeholders detected — fall through to the cached path for parity with query(sql).
            return queryAsJsonValue(sql);
        }
        QueryDefinition resolved = ParameterSubstitutor.substitute(definition, params, settings);
        return executeQuery(resolved);
    }

    /**
     * Executes a parameterised SQL query and maps the single-row result to {@code type}.
     *
     * @param sql SQL SELECT query with placeholders
     * @param type target class
     * @param params bound parameter values
     * @param <T> target type
     * @return mapped instance
     * @since 1.1.0
     */
    public <T> T queryAs(String sql, Class<T> type, BoundParameters params) {
        JsonValue raw = queryAsJsonValue(sql, params);
        return SQL4Json.unwrapAndMap(raw, type, settings);
    }

    /**
     * Executes a parameterised SQL query and maps each result row to {@code type}.
     *
     * @param sql SQL SELECT query with placeholders
     * @param type target class
     * @param params bound parameter values
     * @param <T> element type
     * @return unmodifiable list of mapped instances
     * @since 1.1.0
     */
    public <T> List<T> queryAsList(String sql, Class<T> type, BoundParameters params) {
        JsonValue raw = queryAsJsonValue(sql, params);
        return SQL4Json.mapList(raw, type, settings);
    }

    /** Clear all cached query results. No-op if no cache is configured. */
    public void clearCache() {
        if (cache != null) {
            cache.clear();
        }
    }

    /**
     * Returns the number of entries currently held by the query result cache, or {@code 0} if no cache is configured.
     *
     * @return current cache entry count
     */
    public int cacheSize() {
        return cache != null ? cache.size() : 0;
    }

    /**
     * Returns the {@link Sql4jsonSettings} bound to this engine at build time.
     *
     * @return the bound settings
     */
    public Sql4jsonSettings settings() {
        return settings;
    }
}
