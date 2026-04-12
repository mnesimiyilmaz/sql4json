package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.json.*;
import io.github.mnesimiyilmaz.sql4json.parser.QueryDefinition;
import io.github.mnesimiyilmaz.sql4json.parser.QueryParser;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.settings.DefaultJsonCodecSettings;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Top-level query orchestrator for the v2 pipeline.
 * <p>
 * Responsibilities:
 * - Parse SQL to QueryDefinition (via QueryParser)
 * - Handle FROM subquery recursion
 * - Short-circuit SELECT * FROM $r (no clauses) — zero flatten/unflatten cost
 * - Otherwise: streamLazy → QueryPipeline → JsonUnflattener
 * - Optionally use pre-flattened rows from Engine for faster repeated queries
 * <p>
 * Thread-safe: all fields are immutable after construction.
 * Each execute() call creates its own FieldKey.Interner (query-scoped, GC'd after).
 */
public final class QueryExecutor {

    private static final String           SETTINGS_PARAM = "settings";
    private final        FunctionRegistry functionRegistry;

    /**
     * Creates a new QueryExecutor using the default {@link FunctionRegistry}.
     */
    public QueryExecutor() {
        this.functionRegistry = FunctionRegistry.getDefault();
    }

    /**
     * Execute a pre-parsed QueryDefinition with explicit settings.
     *
     * @param query    the parsed query definition to execute
     * @param data     the JSON data to query against
     * @param settings the settings controlling execution behaviour
     * @return the query result as a {@link JsonValue}
     */
    public JsonValue execute(QueryDefinition query, JsonValue data, Sql4jsonSettings settings) {
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        return executeWithDepth(query, data, List.of(), 0, settings);
    }

    /**
     * Execute pre-parsed query with pre-flattened rows and explicit settings (Engine path).
     * Ensures the Engine's bound {@link Sql4jsonSettings} reaches the executor rather than
     * being silently replaced with defaults.
     *
     * @param query            the parsed query definition to execute
     * @param data             the JSON data to query against
     * @param preFlattenedRows pre-flattened rows from the Engine, or empty list
     * @param settings         the settings controlling execution behaviour
     * @return the query result as a {@link JsonValue}
     */
    public JsonValue execute(QueryDefinition query, JsonValue data, List<Row> preFlattenedRows,
                             Sql4jsonSettings settings) {
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        return executeWithDepth(query, data, preFlattenedRows, 0, settings);
    }

    /**
     * Execute a JOIN query against multiple named data sources with explicit settings.
     * The query's rootPath and each JoinDef's tableName are resolved from the map.
     *
     * @param query       the parsed query definition to execute
     * @param dataSources named data sources keyed by table name
     * @param settings    the settings controlling execution behaviour
     * @return the query result as a {@link JsonValue}
     * @throws io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException if a table name is not found in dataSources
     */
    public JsonValue execute(QueryDefinition query, Map<String, JsonValue> dataSources,
                             Sql4jsonSettings settings) {
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        FieldKey.Interner interner = new FieldKey.Interner();

        // Resolve FROM table
        JsonValue leftData = resolveSource(query.rootPath(), dataSources, settings);
        var leftSource = JoinExecutor.flattenSource(leftData, query.rootAlias(), interner);
        List<Row> currentRows = leftSource.rows();

        // Track schemas for NULL padding (accumulated left-side schema)
        Set<FieldKey> accumulatedLeftSchema = new java.util.LinkedHashSet<>(leftSource.schema());

        // Process each JOIN in order
        if (query.joins() != null) {
            for (var join : query.joins()) {
                JsonValue rightData = resolveSource(join.tableName(), dataSources, settings);
                var rightSource = JoinExecutor.flattenSource(rightData, join.alias(), interner);

                // For RIGHT JOIN, the "opposite schema" is the left-side schema
                Set<FieldKey> oppositeSchema = switch (join.joinType()) {
                    case INNER -> Set.of(); // unused for INNER
                    case LEFT -> rightSource.schema();
                    case RIGHT -> accumulatedLeftSchema;
                };

                currentRows = JoinExecutor.execute(currentRows, rightSource.rows(),
                        oppositeSchema, join, settings.limits().maxRowsPerQuery());

                // After join, accumulate the right-side schema into the left
                accumulatedLeftSchema.addAll(rightSource.schema());
            }
        }

        // Continue with normal pipeline (WHERE → GROUP BY → ...)
        QueryPipeline pipeline = QueryPipeline.build(query, functionRegistry, settings);
        List<Row> result = pipeline.execute(currentRows.stream());

        return JsonUnflattener.unflatten(result, query.selectedColumns(), functionRegistry);
    }

    private static JsonValue resolveSource(String tableName, Map<String, JsonValue> dataSources,
                                           Sql4jsonSettings settings) {
        JsonValue data = dataSources.get(tableName);
        if (data == null) {
            String msg = settings.security().redactErrorDetails()
                    ? "Unknown table"
                    : "Unknown table: '" + tableName + "'. Available: " + dataSources.keySet();
            throw new SQL4JsonExecutionException(msg);
        }
        return data;
    }

    private JsonValue executeWithDepth(QueryDefinition query, JsonValue data,
                                       List<Row> preFlattenedRows,
                                       int currentDepth,
                                       Sql4jsonSettings settings) {
        int maxDepth = settings.limits().maxSubqueryDepth();
        if (currentDepth > maxDepth) {
            throw new SQL4JsonExecutionException(
                    "Subquery depth exceeds configured maximum (" + maxDepth + ")");
        }
        // Resolve FROM subquery recursively — pre-flattened rows don't apply to derived data
        if (query.fromSubQuery() != null) {
            data = executeWithDepth(QueryParser.parse(query.fromSubQuery(), settings),
                    data, List.of(), currentDepth + 1, settings);
            preFlattenedRows = List.of();
        }

        // SHORT-CIRCUIT: SELECT * FROM $r with no filtering/sorting/grouping/limiting
        // Only possible when original data is available (not released by Engine).
        if (data != null
                && query.isSelectAll()
                && query.whereClause() == null
                && query.groupBy() == null
                && query.havingClause() == null
                && query.orderBy() == null
                && query.limit() == null
                && !query.distinct()) {
            JsonValue shortCircuit = JsonFlattener.navigateToPath(data, query.rootPath());
            if (shortCircuit instanceof JsonArrayValue) return shortCircuit;
            return new JsonArrayValue(java.util.List.of(shortCircuit));
        }

        // Use pre-flattened rows if available and FROM is root ($r)
        Stream<Row> rows;
        if (!preFlattenedRows.isEmpty()
                && (query.rootPath() == null || query.rootPath().equals("$r"))) {
            rows = preFlattenedRows.stream();
        } else {
            FieldKey.Interner interner = new FieldKey.Interner();
            rows = JsonFlattener.streamLazy(data, query.rootPath(), interner);
        }

        QueryPipeline pipeline = QueryPipeline.build(query, functionRegistry, settings);
        List<Row> result = pipeline.execute(rows);

        return JsonUnflattener.unflatten(result, query.selectedColumns(), functionRegistry);
    }

    // ── Streaming entry points ──────────────────────────────────────────────

    /**
     * Execute a SQL query against raw JSON string using streaming.
     * Returns serialized String via StreamingSerializer.
     *
     * @param sql        the SQL query string
     * @param jsonString the raw JSON string to query against
     * @param settings   the settings controlling execution behaviour
     * @return the query result serialized as a JSON string
     */
    public String executeStreaming(String sql, String jsonString, Sql4jsonSettings settings) {
        return executeStreaming(QueryParser.parse(sql, settings), jsonString, settings);
    }

    /**
     * Execute a pre-parsed query against raw JSON string using streaming.
     *
     * @param query      the parsed query definition to execute
     * @param jsonString the raw JSON string to query against
     * @param settings   the settings controlling execution behaviour
     * @return the query result serialized as a JSON string
     */
    public String executeStreaming(QueryDefinition query, String jsonString, Sql4jsonSettings settings) {
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        FieldKey.Interner interner = new FieldKey.Interner();
        Stream<Row> rows = resolveStreamingRows(query, jsonString, settings, interner, 0);
        QueryPipeline pipeline = QueryPipeline.build(query, functionRegistry, settings);
        Stream<Row> result = pipeline.executeAsStream(rows);
        return StreamingSerializer.serialize(result, query.selectedColumns(), functionRegistry,
                settings.limits().maxRowsPerQuery());
    }

    /**
     * Execute a query against raw JSON string using streaming.
     * Returns JsonValue (collects streamed results into JsonArrayValue).
     *
     * @param sql        the SQL query string
     * @param jsonString the raw JSON string to query against
     * @param settings   the settings controlling execution behaviour
     * @return the query result as a {@link JsonValue}
     */
    public JsonValue executeStreamingAsJsonValue(String sql, String jsonString, Sql4jsonSettings settings) {
        Objects.requireNonNull(settings, SETTINGS_PARAM);
        return executeStreamingAsJsonValueInternal(QueryParser.parse(sql, settings), jsonString, settings, 0);
    }

    private JsonValue executeStreamingAsJsonValueInternal(QueryDefinition query, String jsonString,
                                                          Sql4jsonSettings settings, int currentDepth) {
        int maxDepth = settings.limits().maxSubqueryDepth();
        if (currentDepth > maxDepth) {
            throw new SQL4JsonExecutionException(
                    "Subquery depth exceeds configured maximum (" + maxDepth + ")");
        }
        FieldKey.Interner interner = new FieldKey.Interner();
        Stream<Row> rows = resolveStreamingRows(query, jsonString, settings, interner, currentDepth);
        QueryPipeline pipeline = QueryPipeline.build(query, functionRegistry, settings);
        List<Row> result = pipeline.execute(rows);
        return JsonUnflattener.unflatten(result, query.selectedColumns(), functionRegistry);
    }

    private Stream<Row> resolveStreamingRows(QueryDefinition query, String jsonString,
                                             Sql4jsonSettings settings,
                                             FieldKey.Interner interner,
                                             int currentDepth) {
        // Handle FROM subquery: inner query streams and materializes to JsonValue,
        // outer query uses tree path on the smaller result.
        if (query.fromSubQuery() != null) {
            QueryDefinition innerQuery = QueryParser.parse(query.fromSubQuery(), settings);
            JsonValue innerResult = executeStreamingAsJsonValueInternal(
                    innerQuery, jsonString, settings, currentDepth + 1);
            return JsonFlattener.streamLazy(innerResult, null, interner);
        }
        DefaultJsonCodecSettings parserSettings = (settings.codec() instanceof DefaultJsonCodec dc)
                ? dc.settings()
                : DefaultJsonCodecSettings.defaults();
        return StreamingJsonParser.streamArray(jsonString, query.rootPath(), parserSettings)
                .map(element -> Row.lazy(element, interner));
    }
}
