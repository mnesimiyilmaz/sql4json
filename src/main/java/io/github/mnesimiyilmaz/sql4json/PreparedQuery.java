package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.engine.ParameterSubstitutor;
import io.github.mnesimiyilmaz.sql4json.engine.QueryExecutor;
import io.github.mnesimiyilmaz.sql4json.json.DefaultJsonCodec;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonCodec;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.List;
import java.util.Map;

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

    /**
     * Executes this prepared query with parameter bindings. Placeholders in the SQL
     * ({@code ?} or {@code :name}) are resolved from {@code params} via
     * {@code ParameterSubstitutor} before execution.
     *
     * <p>Parameterised queries always go through the tree-execution path — the streaming
     * path is skipped because substitution produces a resolved definition that the executor
     * consumes directly.</p>
     *
     * @param jsonData JSON string to execute against
     * @param params   bound parameter values
     * @return result serialized as a JSON string
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException      if a placeholder
     *                                                                               is missing a bind, a bound value has an unsupported type, or IN-list expansion
     *                                                                               exceeds {@code maxInListSize}
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException on other
     *                                                                               execution errors
     * @since 1.1.0
     */
    public String execute(String jsonData, BoundParameters params) {
        QueryDefinition resolved = ParameterSubstitutor.substitute(definition, params, settings);
        JsonCodec codec = settings.codec();
        JsonValue data = codec.parse(jsonData);
        JsonValue result = executor.execute(resolved, data, settings);
        return codec.serialize(result);
    }

    /**
     * Executes this prepared query against a pre-parsed {@link JsonValue} with parameter bindings.
     *
     * @param data   pre-parsed JSON data
     * @param params bound parameter values
     * @return result as a {@link JsonValue}
     * @since 1.1.0
     */
    public JsonValue execute(JsonValue data, BoundParameters params) {
        QueryDefinition resolved = ParameterSubstitutor.substitute(definition, params, settings);
        return executor.execute(resolved, data, settings);
    }

    /**
     * Positional-parameter shortcut: each varargs element is one positional value.
     *
     * <p><b>Footgun:</b> passing a raw {@code Object[]} or primitive array spreads its elements
     * across placeholder slots (Java varargs semantics). To bind a single {@link java.util.List}
     * value, use {@link #execute(String, BoundParameters)} with {@link BoundParameters#of(Object...)}.
     *
     * @param jsonData         JSON string to execute against
     * @param positionalParams positional parameter values (one per {@code ?} placeholder)
     * @return result serialized as a JSON string
     * @since 1.1.0
     */
    public String execute(String jsonData, Object... positionalParams) {
        return execute(jsonData, BoundParameters.of(positionalParams));
    }

    /**
     * Named-parameter shortcut: map entries become named bindings.
     *
     * @param jsonData    JSON string to execute against
     * @param namedParams map of {@code name → value} bindings
     * @return result serialized as a JSON string
     * @since 1.1.0
     */
    public String execute(String jsonData, Map<String, ?> namedParams) {
        return execute(jsonData, BoundParameters.of(namedParams));
    }

    /**
     * Execute this prepared query against JSON string data and map the single-row result to
     * an instance of {@code type}.
     *
     * <p>The result must be a single-row array. Use {@link #executeAsList} when multiple rows
     * are expected.</p>
     *
     * @param jsonData JSON string to execute against
     * @param type     target class to map the result row to
     * @param <T>      result type
     * @return mapped instance of {@code type}
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if an error
     *                                                                               occurs during execution or JSON parsing
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException   if the result
     *                                                                               is not a single row or cannot be mapped to {@code type}
     * @since 1.1.0
     */
    public <T> T executeAs(String jsonData, Class<T> type) {
        JsonValue raw = executor.execute(definition, settings.codec().parse(jsonData), settings);
        return SQL4Json.unwrapAndMap(raw, type, settings);
    }

    /**
     * Execute this prepared query against a {@link JsonValue} and map the single-row result to
     * an instance of {@code type}.
     *
     * <p>The result must be a single-row array. Use {@link #executeAsList(JsonValue, Class)} when
     * multiple rows are expected.</p>
     *
     * @param data pre-parsed JSON data to execute against
     * @param type target class to map the result row to
     * @param <T>  result type
     * @return mapped instance of {@code type}
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if an error
     *                                                                               occurs during execution
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException   if the result
     *                                                                               is not a single row or cannot be mapped to {@code type}
     * @since 1.1.0
     */
    public <T> T executeAs(JsonValue data, Class<T> type) {
        JsonValue raw = executor.execute(definition, data, settings);
        return SQL4Json.unwrapAndMap(raw, type, settings);
    }

    /**
     * Execute this prepared query against JSON string data and map every result row to
     * an instance of {@code type}, returning an unmodifiable list.
     *
     * @param jsonData JSON string to execute against
     * @param type     target class to map each result row to
     * @param <T>      element type
     * @return unmodifiable list of mapped instances (empty list when the result is empty)
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if an error
     *                                                                               occurs during execution or JSON parsing
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException   if the result
     *                                                                               is not an array or a row cannot be mapped to {@code type}
     * @since 1.1.0
     */
    public <T> List<T> executeAsList(String jsonData, Class<T> type) {
        JsonValue raw = executor.execute(definition, settings.codec().parse(jsonData), settings);
        return SQL4Json.mapList(raw, type, settings);
    }

    /**
     * Execute this prepared query against a {@link JsonValue} and map every result row to
     * an instance of {@code type}, returning an unmodifiable list.
     *
     * @param data pre-parsed JSON data to execute against
     * @param type target class to map each result row to
     * @param <T>  element type
     * @return unmodifiable list of mapped instances (empty list when the result is empty)
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if an error
     *                                                                               occurs during execution
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException   if the result
     *                                                                               is not an array or a row cannot be mapped to {@code type}
     * @since 1.1.0
     */
    public <T> List<T> executeAsList(JsonValue data, Class<T> type) {
        JsonValue raw = executor.execute(definition, data, settings);
        return SQL4Json.mapList(raw, type, settings);
    }

    /**
     * Executes this prepared query with parameter bindings against JSON string data and maps
     * the single-row result to an instance of {@code type}.
     *
     * @param jsonData JSON string to execute against
     * @param type     target class to map the result row to
     * @param params   bound parameter values
     * @param <T>      result type
     * @return mapped instance of {@code type}
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException    if binding fails
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException if the result
     *                                                                             is not a single row or cannot be mapped to {@code type}
     * @since 1.1.0
     */
    public <T> T executeAs(String jsonData, Class<T> type, BoundParameters params) {
        QueryDefinition resolved = ParameterSubstitutor.substitute(definition, params, settings);
        JsonValue raw = executor.execute(resolved, settings.codec().parse(jsonData), settings);
        return SQL4Json.unwrapAndMap(raw, type, settings);
    }

    /**
     * Executes this prepared query with parameter bindings against a pre-parsed
     * {@link JsonValue} and maps the single-row result to an instance of {@code type}.
     *
     * @param data   pre-parsed JSON data
     * @param type   target class to map the result row to
     * @param params bound parameter values
     * @param <T>    result type
     * @return mapped instance of {@code type}
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException    if binding fails
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException if the result
     *                                                                             is not a single row or cannot be mapped to {@code type}
     * @since 1.1.0
     */
    public <T> T executeAs(JsonValue data, Class<T> type, BoundParameters params) {
        QueryDefinition resolved = ParameterSubstitutor.substitute(definition, params, settings);
        JsonValue raw = executor.execute(resolved, data, settings);
        return SQL4Json.unwrapAndMap(raw, type, settings);
    }

    /**
     * Executes this prepared query with parameter bindings against JSON string data and maps
     * every result row to an instance of {@code type}.
     *
     * @param jsonData JSON string to execute against
     * @param type     target class to map each row to
     * @param params   bound parameter values
     * @param <T>      element type
     * @return unmodifiable list of mapped instances
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException    if binding fails
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException if a row cannot
     *                                                                             be mapped to {@code type}
     * @since 1.1.0
     */
    public <T> List<T> executeAsList(String jsonData, Class<T> type, BoundParameters params) {
        QueryDefinition resolved = ParameterSubstitutor.substitute(definition, params, settings);
        JsonValue raw = executor.execute(resolved, settings.codec().parse(jsonData), settings);
        return SQL4Json.mapList(raw, type, settings);
    }

    /**
     * Executes this prepared query with parameter bindings against a pre-parsed
     * {@link JsonValue} and maps every result row to an instance of {@code type}.
     *
     * @param data   pre-parsed JSON data
     * @param type   target class to map each row to
     * @param params bound parameter values
     * @param <T>    element type
     * @return unmodifiable list of mapped instances
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException    if binding fails
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonMappingException if a row cannot
     *                                                                             be mapped to {@code type}
     * @since 1.1.0
     */
    public <T> List<T> executeAsList(JsonValue data, Class<T> type, BoundParameters params) {
        QueryDefinition resolved = ParameterSubstitutor.substitute(definition, params, settings);
        JsonValue raw = executor.execute(resolved, data, settings);
        return SQL4Json.mapList(raw, type, settings);
    }
}
