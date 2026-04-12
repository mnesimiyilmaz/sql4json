package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.engine.QueryExecutor;
import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

/**
 * A pre-parsed SQL query that can be executed multiple times against different data
 * without reparsing. Analogous to JDBC {@code PreparedStatement}.
 *
 * <p>A {@code PreparedQuery} holds a bound {@link Sql4jsonSettings} that governs both
 * parse-time limits (applied at construction) and execution-time limits (applied on
 * each {@link #execute} call).</p>
 *
 * <p>Thread-safe: {@link QueryDefinition} is an immutable record, and each
 * {@link #execute} call creates its own Row instances and Interner.</p>
 *
 * <p>Usage example:</p>
 * <pre>{@code
 * // Default settings
 * PreparedQuery q = SQL4Json.prepare("SELECT name FROM $r WHERE age > 25");
 * String r1 = q.execute(json1);
 * String r2 = q.execute(json2);
 *
 * // Custom settings — limits apply to every execute() call
 * Sql4jsonSettings settings = Sql4jsonSettings.builder()
 *     .limits(l -> l.maxRowsPerQuery(10_000))
 *     .build();
 * PreparedQuery q = SQL4Json.prepare("SELECT name FROM $r WHERE age > 25", settings);
 * String result = q.execute(json1);
 * }</pre>
 */
public final class PreparedQuery {

    private final QueryDefinition  definition;
    private final QueryExecutor    executor;
    private final Sql4jsonSettings settings;

    PreparedQuery(String sql, QueryExecutor executor, Sql4jsonSettings settings) {
        this.settings = settings;
        this.definition = QueryParser.parse(sql, settings);
        this.executor = executor;
    }

    /**
     * Execute this prepared query against JSON string data.
     * Execution limits are governed by the {@link Sql4jsonSettings} bound at construction time.
     *
     * @param jsonData JSON string to execute against
     * @return result serialized as a JSON string
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if an error
     *                                                                               occurs during execution or JSON parsing, including when a configured limit from
     *                                                                               {@link Sql4jsonSettings} is exceeded (subquery depth, materialized row count, or
     *                                                                               JSON codec limits when using the default codec)
     */
    public String execute(String jsonData) {
        JsonCodec codec = settings.codec();
        if (codec instanceof DefaultJsonCodec) {
            return executor.executeStreaming(definition, jsonData, settings);
        }
        JsonValue data = codec.parse(jsonData);
        JsonValue result = executor.execute(definition, data, settings);
        return codec.serialize(result);
    }

    /**
     * Execute this prepared query against a {@link JsonValue}.
     * Execution limits are governed by the {@link Sql4jsonSettings} bound at construction time.
     *
     * @param data pre-parsed JSON data to execute against
     * @return result as a {@code JsonValue}
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if an error
     *                                                                               occurs during execution, including when a configured limit from
     *                                                                               {@link Sql4jsonSettings} is exceeded (subquery depth or materialized row count)
     */
    public JsonValue execute(JsonValue data) {
        return executor.execute(definition, data, settings);
    }
}
