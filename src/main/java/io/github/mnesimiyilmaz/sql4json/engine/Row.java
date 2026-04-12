package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.json.*;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;
import io.github.mnesimiyilmaz.sql4json.types.SqlNull;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.*;
import java.util.stream.Stream;

/**
 * A row in the query execution pipeline, holding a flat view of one JSON element.
 *
 * <p>Rows are either <em>lazy</em> (created from a {@link io.github.mnesimiyilmaz.sql4json.types.JsonValue}
 * and resolved on demand) or <em>eager</em> (pre-populated, used for GROUP BY results).
 *
 * <p><strong>Not thread-safe.</strong> Each Row instance must be accessed from a single thread.
 * Phase 5 must ensure the execution pipeline does not share Row instances across threads.
 */
public final class Row {

    private final JsonValue               original;
    private final Map<FieldKey, SqlValue> cache;
    private final FieldKey.Interner       interner;
    private       boolean                 fullyFlattened;
    private final boolean                 modified;
    private final List<Row>               sourceGroup;
    private       Map<FieldKey, SqlValue> windowResults;

    private Row(JsonValue original, Map<FieldKey, SqlValue> cache,
                FieldKey.Interner interner, boolean fullyFlattened, boolean modified,
                List<Row> sourceGroup) {
        this.original = original;
        this.cache = cache;
        this.interner = interner;
        this.fullyFlattened = fullyFlattened;
        this.modified = modified;
        this.sourceGroup = sourceGroup;
    }

    /**
     * Lazy Row — created from a JSON element; no upfront flatten.
     *
     * @param original the original JSON value for this row
     * @param interner the interner for deduplicating FieldKey instances
     * @return a new lazy Row
     */
    public static Row lazy(JsonValue original, FieldKey.Interner interner) {
        return new Row(original, HashMap.newHashMap(4), interner, false, false, null);
    }

    /**
     * Pre-flattened Row — created at Engine build time; fully flattened, shared across queries.
     * Does not retain a reference to the original JsonValue, allowing the parsed tree to be GC'd.
     * The fields map should be unmodifiable to ensure thread-safety across concurrent queries.
     *
     * @param fields the pre-flattened field map
     * @return a new pre-flattened Row
     */
    public static Row preFlattened(Map<FieldKey, SqlValue> fields) {
        return new Row(null, fields, null, true, false, null);
    }

    /**
     * Eager Row — created from flat data (GROUP BY output, aggregation results).
     *
     * @param data the flat field data
     * @return a new eager Row
     */
    public static Row eager(Map<FieldKey, SqlValue> data) {
        return new Row(null, Collections.unmodifiableMap(data), null, true, true, null);
    }

    /**
     * Eager Row with source group — created by GROUP BY so that HAVING can
     * re-evaluate aggregate expressions (e.g. ROUND(AVG(val), 0) &gt; 50).
     *
     * @param data        the flat field data
     * @param sourceGroup the source rows that were aggregated into this row
     * @return a new eager Row with the given source group
     */
    public static Row eager(Map<FieldKey, SqlValue> data, List<Row> sourceGroup) {
        return new Row(null, Collections.unmodifiableMap(data), null, true, true,
                List.copyOf(sourceGroup));
    }

    /**
     * Returns the source group of rows that produced this aggregated row,
     * or empty if this is not a GROUP BY result row.
     *
     * @return an optional containing the source group, or empty
     */
    public Optional<List<Row>> sourceGroup() {
        return Optional.ofNullable(sourceGroup);
    }

    /**
     * Store a window function result for this row.
     * Window results are stored in a separate map so they don't conflict with
     * the unmodifiable cache of pre-flattened/eager rows.
     *
     * @param key   the field key identifying the window result
     * @param value the computed window function value
     */
    public void putWindowResult(FieldKey key, SqlValue value) {
        if (windowResults == null) windowResults = HashMap.newHashMap(4);
        windowResults.put(key, value);
    }

    /**
     * Retrieve the value for the given field key, resolving lazily if needed.
     *
     * @param key the field key to look up
     * @return the value, or {@link SqlNull#INSTANCE} if absent
     */
    public SqlValue get(FieldKey key) {
        // 1. Window results take precedence (computed by WindowStage)
        if (windowResults != null) {
            SqlValue wr = windowResults.get(key);
            if (wr != null) return wr;
        }

        // 2. Cache hit
        SqlValue cached = cache.get(key);
        if (cached != null) return cached;

        // 3. Already fully flattened (or no original) — key absent → SqlNull
        if (fullyFlattened || original == null) return SqlNull.INSTANCE;

        // 4. Lazy resolve from original JsonValue
        SqlValue resolved = navigateAndConvert(original, key.getKey());
        cache.put(key, resolved);
        return resolved;
    }

    /**
     * Force full flatten — called by GROUP BY which needs all fields.
     *
     * @return this row, now fully flattened
     */
    public Row ensureFullyFlattened() {
        if (!fullyFlattened && original != null) {
            JsonFlattener.flattenInto(original, cache, interner);
            fullyFlattened = true;
        }
        return this;
    }

    /**
     * Original JsonValue reference — enables cherry-pick unflatten.
     *
     * @return an optional containing the original JsonValue, or empty for eager/pre-flattened rows
     */
    public Optional<JsonValue> originalValue() {
        return Optional.ofNullable(original);
    }

    /**
     * Returns whether this row has been modified (e.g. produced by GROUP BY or aggregation).
     *
     * @return {@code true} if this row was created from aggregation or projection
     */
    public boolean isModified() {
        return modified;
    }

    /**
     * Returns a stream of all field entries after ensuring the row is fully flattened.
     *
     * @return a stream of field key-value entries
     */
    public Stream<Map.Entry<FieldKey, SqlValue>> entries() {
        ensureFullyFlattened();
        return cache.entrySet().stream();
    }

    /**
     * Returns the set of all field keys after ensuring the row is fully flattened.
     *
     * @return the set of field keys
     */
    public Set<FieldKey> keys() {
        ensureFullyFlattened();
        return cache.keySet();
    }

    /**
     * Creates a new Row projected to the given set of field keys.
     *
     * @param keys the field keys to retain
     * @return a new Row containing only the specified keys
     */
    public Row project(Set<FieldKey> keys) {
        var projected = new HashMap<FieldKey, SqlValue>();
        keys.forEach(k -> projected.put(k, get(k)));
        Row result = new Row(original, projected, interner, true, modified, sourceGroup);
        if (windowResults != null) result.windowResults = new HashMap<>(windowResults);
        return result;
    }

    /**
     * Returns all values whose field key belongs to the given family.
     *
     * @param family the family prefix to filter by
     * @return a list of values matching the family
     */
    public List<SqlValue> valuesByFamily(String family) {
        ensureFullyFlattened();
        return cache.entrySet().stream()
                .filter(e -> e.getKey().getFamily().equals(family))
                .map(Map.Entry::getValue)
                .toList();
    }

    // Test helper — allows tests to verify lazy resolution
    int cachedFieldCount() {
        return cache.size();
    }

    // Navigate dot-separated path on original JsonValue and convert to SqlValue
    private static SqlValue navigateAndConvert(JsonValue root, String path) {
        JsonValue current = root;
        for (String segment : path.split("\\.")) {
            int bracketIdx = segment.indexOf('[');
            if (bracketIdx >= 0) {
                String field = segment.substring(0, bracketIdx);
                int arrayIdx = Integer.parseInt(
                        segment.substring(bracketIdx + 1, segment.length() - 1));
                current = current.asObject()
                        .map(m -> m.get(field))
                        .flatMap(JsonValue::asArray)
                        .map(arr -> arrayIdx < arr.size() ? arr.get(arrayIdx) : JsonNullValue.INSTANCE)
                        .orElse(JsonNullValue.INSTANCE);
            } else {
                current = current.asObject()
                        .map(m -> m.getOrDefault(segment, JsonNullValue.INSTANCE))
                        .orElse(JsonNullValue.INSTANCE);
            }
        }
        // Object and array values cannot be converted to SqlValue for WHERE/HAVING comparisons.
        // Return SqlNull so that cherryPick in JsonUnflattener can still access the original.
        if (current instanceof JsonObjectValue || current instanceof JsonArrayValue) {
            return SqlNull.INSTANCE;
        }
        return JsonToSqlConverter.toSqlValue(current);
    }

}
