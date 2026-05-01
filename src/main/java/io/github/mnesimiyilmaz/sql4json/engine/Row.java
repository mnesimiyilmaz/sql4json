package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.internal.SkipCoverageGenerated;
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
 * and resolved on demand) or <em>eager</em> (pre-populated, used for the small
 * literal-evaluation seed in {@code WindowStage.resolveOffset}).
 *
 * <p><strong>Not thread-safe.</strong> Each Row instance must be accessed from a single thread —
 * the execution pipeline never shares Row instances across threads.
 *
 * <p>As of 1.2.0 the pipeline-shared materialised row is {@link FlatRow} —
 * {@code Row} is reserved for lazy access during streaming flatten and the
 * narrow seed-row use cases listed above.</p>
 */
public final class Row implements RowAccessor {

    private final JsonValue               original;
    private final Map<FieldKey, SqlValue> cache;
    private final FieldKey.Interner       interner;
    private       boolean                 fullyFlattened;
    private final boolean                 modified;

    private Row(JsonValue original, Map<FieldKey, SqlValue> cache,
                FieldKey.Interner interner, boolean fullyFlattened, boolean modified) {
        this.original = original;
        this.cache = cache;
        this.interner = interner;
        this.fullyFlattened = fullyFlattened;
        this.modified = modified;
    }

    /**
     * Lazy Row — created from a JSON element; no upfront flatten.
     *
     * @param original the original JSON value for this row
     * @param interner the interner for deduplicating FieldKey instances
     * @return a new lazy Row
     */
    @SkipCoverageGenerated
    public static Row lazy(JsonValue original, FieldKey.Interner interner) {
        // Pre-size the cache to the original object's top-level field count so
        // ensureFullyFlattened() (called by GROUP BY / WINDOW) can populate
        // without resizing the backing table 3-4 times for a typical 13-field
        // row. Streaming WHERE / lazy SELECT only caches a few fields and
        // tolerates the small fixed slack in exchange for the win on materializing
        // pipelines (HashMap$Node[] dominated allocations on the GROUP BY profile).
        int initialCap = 8;
        if (original instanceof JsonObjectValue(var fields)) {
            initialCap = Math.max(8, fields.size() + (fields.size() >>> 1));
        }
        return new Row(original, HashMap.newHashMap(initialCap), interner, false, false);
    }

    /**
     * Eager Row — created from flat data that still represents raw input fields.
     * As of 1.2.0 this is reserved for the literal-evaluation seed in
     * {@link io.github.mnesimiyilmaz.sql4json.engine.stage.WindowStage} and tests
     * that need a tiny in-memory row.
     *
     * @param data the flat field data
     * @return a new eager Row whose values are raw, not pre-evaluated against SELECT
     */
    public static Row eager(Map<FieldKey, SqlValue> data) {
        return new Row(null, Collections.unmodifiableMap(data), null, true, true);
    }

    /**
     * Lazy rows have no schema — they carry no precomputed values, and consumers
     * resolve fields on demand via {@link #get(FieldKey)}. Window stages and the
     * unflattener detect lazy rows by checking for a {@code null} schema.
     *
     * @return always {@code null}
     */
    @Override
    public RowSchema schema() {
        return null;
    }

    /**
     * Lazy rows do not source from GROUP BY aggregation; this always returns empty.
     *
     * @return always empty
     */
    @Override
    public Optional<List<RowAccessor>> sourceGroup() {
        return Optional.empty();
    }

    /**
     * Lazy rows do not carry window-function results. {@code WindowStage} emits
     * {@link FlatRow} directly; window-result lookups against a lazy row always
     * return {@code null} so callers can distinguish "no window stage ran" from
     * "no slot for this call".
     *
     * @param windowCall the window function call to look up
     * @return always {@code null}
     * @since 1.2.0
     */
    @Override
    public SqlValue getWindowResult(Expression.WindowFnCall windowCall) {
        return null;
    }

    /**
     * Lazy rows never carry window results.
     *
     * @return always {@code false}
     * @since 1.2.0
     */
    @Override
    public boolean hasWindowResults() {
        return false;
    }

    /**
     * Retrieve the value for the given field key, resolving lazily if needed.
     *
     * @param key the field key to look up
     * @return the value, or {@link SqlNull#INSTANCE} if absent
     */
    @Override
    public SqlValue get(FieldKey key) {
        // 1. Cache hit
        SqlValue cached = cache.get(key);
        if (cached != null) return cached;

        // 2. Already fully flattened (or no original) — key absent → SqlNull
        if (fullyFlattened || original == null) return SqlNull.INSTANCE;

        // 3. Lazy resolve from original JsonValue
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
    @Override
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
     * Lazy rows are never aggregated. GROUP BY now produces {@link FlatRow}
     * directly; aggregated state lives there.
     *
     * @return always {@code false}
     */
    @Override
    public boolean isAggregated() {
        return false;
    }

    /**
     * Returns a stream of all field entries after ensuring the row is fully flattened.
     *
     * @return a stream of field key-value entries
     */
    @Override
    public Stream<Map.Entry<FieldKey, SqlValue>> entries() {
        ensureFullyFlattened();
        return cache.entrySet().stream();
    }

    /**
     * Returns the set of all field keys after ensuring the row is fully flattened.
     *
     * @return the set of field keys
     */
    @Override
    public Set<FieldKey> keys() {
        ensureFullyFlattened();
        return cache.keySet();
    }

    /**
     * Creates a new Row projected to the given set of field keys. Used by tests
     * exercising lazy-row projection; production stages now project to
     * {@link FlatRow} via {@link io.github.mnesimiyilmaz.sql4json.engine.stage.SelectStage}.
     *
     * @param keys the field keys to retain
     * @return a new Row containing only the specified keys
     */
    public Row project(Set<FieldKey> keys) {
        var projected = new HashMap<FieldKey, SqlValue>();
        keys.forEach(k -> projected.put(k, get(k)));
        return new Row(original, projected, interner, true, modified);
    }

    /**
     * Returns all values whose field key belongs to the given family.
     *
     * @param family the family prefix to filter by
     * @return a list of values matching the family
     */
    @Override
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
