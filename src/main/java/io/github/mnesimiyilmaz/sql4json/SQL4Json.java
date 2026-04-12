package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.engine.QueryExecutor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonException;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Main entry point for SQL4Json
 *
 * <p>Usage patterns:</p>
 * <pre>{@code
 * // One-off query with defaults
 * String result = SQL4Json.query("SELECT name FROM $r WHERE age > 25", jsonString);
 *
 * // One-off query with custom limits
 * Sql4jsonSettings strict = Sql4jsonSettings.builder()
 *     .limits(l -> l.maxRowsPerQuery(10_000))
 *     .build();
 * String result = SQL4Json.query(sql, jsonString, strict);
 *
 * // Prepared query — parse once, execute many
 * PreparedQuery q = SQL4Json.prepare("SELECT name FROM $r WHERE age > 25");
 * String r1 = q.execute(json1);
 * String r2 = q.execute(json2);
 *
 * // Engine with bound data and result cache
 * SQL4JsonEngine engine = SQL4Json.engine()
 *     .settings(Sql4jsonSettings.builder()
 *         .cache(c -> c.queryResultCacheEnabled(true))
 *         .build())
 *     .data(jsonString)
 *     .build();
 * String result = engine.query(sql);
 * }</pre>
 */
public final class SQL4Json {

    private static final QueryExecutor EXECUTOR       = new QueryExecutor();
    private static final String        SETTINGS_PARAM = "settings";

    private SQL4Json() {
    }

    /**
     * Execute a SQL query against JSON data, returning the result as a JSON string.
     * Parsing and execution limits are governed by {@link Sql4jsonSettings#defaults()}.
     * Equivalent to {@link #query(String, String, Sql4jsonSettings) query(sql, jsonData, Sql4jsonSettings.defaults())}.
     *
     * @param sql      SQL SELECT query
     * @param jsonData JSON string to query against
     * @return result serialized as a JSON string
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth, materialized row count, and JSON
     *                                    codec limits at execution time)
     */
    public static String query(String sql, String jsonData) {
        return query(sql, jsonData, Sql4jsonSettings.defaults());
    }

    /**
     * Execute a SQL query against JSON data with explicit settings, returning the result as a JSON string.
     * Parsing and execution limits are governed by the supplied {@link Sql4jsonSettings}.
     *
     * @param sql      SQL SELECT query
     * @param jsonData JSON string to query against
     * @param settings settings controlling limits, codec, cache, and security behaviour
     * @return result serialized as a JSON string
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth, materialized row count, and JSON
     *                                    codec limits at execution time)
     */
    public static String query(String sql, String jsonData, Sql4jsonSettings settings) {
        validateQuery(sql);
        validateData(jsonData);
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        JsonCodec codec = settings.codec();
        if (codec instanceof DefaultJsonCodec) {
            return EXECUTOR.executeStreaming(sql, jsonData, settings);
        }
        JsonValue data = codec.parse(jsonData);
        JsonValue result = EXECUTOR.execute(QueryParser.parse(sql, settings), data, settings);
        return codec.serialize(result);
    }

    /**
     * Execute a SQL query against JSON data, returning the result as a {@link JsonValue}.
     * Parsing and execution limits are governed by {@link Sql4jsonSettings#defaults()}.
     * Equivalent to {@link #queryAsJsonValue(String, String, Sql4jsonSettings)
     * queryAsJsonValue(sql, jsonData, Sql4jsonSettings.defaults())}.
     *
     * @param sql      SQL SELECT query
     * @param jsonData JSON string to query against
     * @return result as a {@code JsonValue}
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth, materialized row count, and JSON
     *                                    codec limits at execution time)
     */
    public static JsonValue queryAsJsonValue(String sql, String jsonData) {
        return queryAsJsonValue(sql, jsonData, Sql4jsonSettings.defaults());
    }

    /**
     * Execute a SQL query against JSON data with explicit settings, returning the result as a {@link JsonValue}.
     * Parsing and execution limits are governed by the supplied {@link Sql4jsonSettings}.
     *
     * @param sql      SQL SELECT query
     * @param jsonData JSON string to query against
     * @param settings settings controlling limits, codec, cache, and security behaviour
     * @return result as a {@code JsonValue}
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth, materialized row count, and JSON
     *                                    codec limits at execution time)
     */
    public static JsonValue queryAsJsonValue(String sql, String jsonData, Sql4jsonSettings settings) {
        validateQuery(sql);
        validateData(jsonData);
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        JsonCodec codec = settings.codec();
        if (codec instanceof DefaultJsonCodec) {
            return EXECUTOR.executeStreamingAsJsonValue(sql, jsonData, settings);
        }
        JsonValue data = codec.parse(jsonData);
        return EXECUTOR.execute(QueryParser.parse(sql, settings), data, settings);
    }

    /**
     * Execute a SQL query against a {@link JsonValue}, returning the result as a JsonValue.
     * Parsing and execution limits are governed by {@link Sql4jsonSettings#defaults()}.
     * Equivalent to {@link #queryAsJsonValue(String, JsonValue, Sql4jsonSettings)
     * queryAsJsonValue(sql, data, Sql4jsonSettings.defaults())}.
     *
     * @param sql  SQL SELECT query
     * @param data pre-parsed JSON data to query against
     * @return result as a {@code JsonValue}
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth and materialized row count at
     *                                    execution time)
     */
    public static JsonValue queryAsJsonValue(String sql, JsonValue data) {
        return queryAsJsonValue(sql, data, Sql4jsonSettings.defaults());
    }

    /**
     * Execute a SQL query against a {@link JsonValue} with explicit settings, returning the result as a JsonValue.
     * Parsing and execution limits are governed by the supplied {@link Sql4jsonSettings}.
     *
     * @param sql      SQL SELECT query
     * @param data     pre-parsed JSON data to query against
     * @param settings settings controlling limits, codec, cache, and security behaviour
     * @return result as a {@code JsonValue}
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth and materialized row count at
     *                                    execution time)
     */
    public static JsonValue queryAsJsonValue(String sql, JsonValue data, Sql4jsonSettings settings) {
        validateQuery(sql);
        if (data == null) {
            throw new SQL4JsonException("Input data must not be null");
        }
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        return EXECUTOR.execute(QueryParser.parse(sql, settings), data, settings);
    }

    /**
     * Execute a JOIN query against multiple named JSON data sources.
     * Parsing and execution limits are governed by {@link Sql4jsonSettings#defaults()}.
     * Equivalent to {@link #query(String, Map, Sql4jsonSettings) query(sql, dataSources, Sql4jsonSettings.defaults())}.
     *
     * @param sql         SQL SELECT query with JOIN clauses
     * @param dataSources map of table name to JSON string
     * @return result as JSON string
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth, materialized row count, and JSON
     *                                    codec limits at execution time)
     */
    public static String query(String sql, Map<String, String> dataSources) {
        return query(sql, dataSources, Sql4jsonSettings.defaults());
    }

    /**
     * Execute a JOIN query against multiple named JSON data sources with explicit settings.
     * Parsing and execution limits are governed by the supplied {@link Sql4jsonSettings}.
     *
     * @param sql         SQL SELECT query with JOIN clauses
     * @param dataSources map of table name to JSON string
     * @param settings    settings controlling limits, codec, and other behaviour
     * @return result as JSON string
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth, materialized row count, and JSON
     *                                    codec limits at execution time)
     */
    public static String query(String sql, Map<String, String> dataSources, Sql4jsonSettings settings) {
        validateQuery(sql);
        if (dataSources == null || dataSources.isEmpty()) {
            throw new SQL4JsonException("Data sources must not be null or empty");
        }
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        JsonValue result = queryAsJsonValue(sql, dataSources, settings);
        return settings.codec().serialize(result);
    }

    /**
     * Execute a JOIN query against multiple named JSON data sources.
     * Parsing and execution limits are governed by {@link Sql4jsonSettings#defaults()}.
     * Equivalent to {@link #queryAsJsonValue(String, Map, Sql4jsonSettings)
     * queryAsJsonValue(sql, dataSources, Sql4jsonSettings.defaults())}.
     *
     * @param sql         SQL SELECT query with JOIN clauses
     * @param dataSources map of table name to JSON string
     * @return result as JsonValue
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth, materialized row count, and JSON
     *                                    codec limits at execution time)
     */
    public static JsonValue queryAsJsonValue(String sql, Map<String, String> dataSources) {
        return queryAsJsonValue(sql, dataSources, Sql4jsonSettings.defaults());
    }

    /**
     * Execute a JOIN query against multiple named JSON data sources with explicit settings.
     * Parsing and execution limits are governed by the supplied {@link Sql4jsonSettings}.
     *
     * @param sql         SQL SELECT query with JOIN clauses
     * @param dataSources map of table name to JSON string
     * @param settings    settings controlling limits, codec, and other behaviour
     * @return result as JsonValue
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if an error occurs during parsing or execution, including
     *                                    when a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded (IN list size and LIKE wildcard count at parse
     *                                    time; subquery depth, materialized row count, and JSON
     *                                    codec limits at execution time)
     */
    public static JsonValue queryAsJsonValue(String sql, Map<String, String> dataSources,
                                             Sql4jsonSettings settings) {
        validateQuery(sql);
        if (dataSources == null || dataSources.isEmpty()) {
            throw new SQL4JsonException("Data sources must not be null or empty");
        }
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        JsonCodec codec = settings.codec();
        Map<String, JsonValue> parsed = new LinkedHashMap<>();
        dataSources.forEach((name, json) -> {
            if (json == null) {
                throw new SQL4JsonException("Data for source '" + name + "' must not be null");
            }
            parsed.put(name, codec.parse(json));
        });
        QueryDefinition definition = QueryParser.parse(sql, settings);
        return EXECUTOR.execute(definition, parsed, settings);
    }

    /**
     * Prepare a SQL query for repeated execution against different data.
     * Parses the SQL once using {@link Sql4jsonSettings#defaults()}; subsequent
     * {@link PreparedQuery#execute} calls skip parsing.
     * Equivalent to {@link #prepare(String, Sql4jsonSettings) prepare(sql, Sql4jsonSettings.defaults())}.
     *
     * @param sql SQL SELECT query
     * @return a reusable, thread-safe PreparedQuery
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded at parse time (e.g. IN list size or LIKE
     *                                    wildcard count)
     */
    public static PreparedQuery prepare(String sql) {
        return prepare(sql, Sql4jsonSettings.defaults());
    }

    /**
     * Prepare a SQL query for repeated execution against different data with explicit settings.
     * Parses the SQL once; subsequent {@link PreparedQuery#execute} calls skip parsing.
     * The bound {@link Sql4jsonSettings} governs both parse-time and execution-time limits for all
     * subsequent {@link PreparedQuery#execute} calls.
     *
     * @param sql      SQL SELECT query
     * @param settings settings controlling limits, codec, and other behaviour
     * @return a reusable, thread-safe PreparedQuery
     * @throws SQL4JsonParseException     if the SQL has syntax errors or exceeds the configured
     *                                    SQL length limit (see {@link Sql4jsonSettings})
     * @throws SQL4JsonExecutionException if a configured limit from {@link Sql4jsonSettings} is
     *                                    exceeded at parse time (e.g. IN list size or LIKE
     *                                    wildcard count)
     */
    public static PreparedQuery prepare(String sql, Sql4jsonSettings settings) {
        validateQuery(sql);
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        return new PreparedQuery(sql, EXECUTOR, settings);
    }

    /**
     * Create a builder for a new {@link SQL4JsonEngine}.
     *
     * @return a new engine builder
     */
    public static SQL4JsonEngineBuilder engine() {
        return new SQL4JsonEngineBuilder();
    }

    static void validateQuery(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new SQL4JsonException("SQL query must not be null or blank");
        }
    }

    private static void validateData(String jsonData) {
        if (jsonData == null) {
            throw new SQL4JsonException("Input data must not be null");
        }
    }
}
