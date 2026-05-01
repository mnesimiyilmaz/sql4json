package io.github.mnesimiyilmaz.sql4json.engine.stage;

import io.github.mnesimiyilmaz.sql4json.engine.FieldKey;
import io.github.mnesimiyilmaz.sql4json.engine.MaterializingPipelineStage;
import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.sorting.SqlValueComparator;
import io.github.mnesimiyilmaz.sql4json.types.*;

import java.util.*;
import java.util.stream.Stream;

/**
 * Materializing pipeline stage that eliminates duplicate rows (SQL DISTINCT).
 * Uses value-based equality via {@link SqlValueComparator}.
 */
public final class DistinctStage implements MaterializingPipelineStage {

    private final int maxRows;

    /**
     * Creates a new DistinctStage with the specified row limit.
     *
     * @param maxRows maximum number of distinct rows before throwing
     */
    public DistinctStage(int maxRows) {
        this.maxRows = maxRows;
    }

    @Override
    public Stream<RowAccessor> apply(Stream<RowAccessor> input) {
        Map<DistinctKey, RowAccessor> seen = new LinkedHashMap<>();
        input.forEach(row -> {
            DistinctKey key = DistinctKey.of(row);
            seen.computeIfAbsent(key, k -> {
                if (seen.size() >= maxRows) {
                    throw new SQL4JsonExecutionException(
                            "DISTINCT row count exceeds configured maximum (" + maxRows + ")");
                }
                return row;
            });
        });
        return seen.values().stream();
    }

    private record DistinctKey(List<SqlValue> values) {
        static DistinctKey of(RowAccessor row) {
            List<SqlValue> vals = new ArrayList<>();
            row.entries()
                    .sorted(Map.Entry.comparingByKey(
                            Comparator.comparing(FieldKey::getKey)))
                    .forEach(e -> vals.add(e.getValue()));
            return new DistinctKey(vals);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DistinctKey(List<SqlValue> valueList))) return false;
            if (values.size() != valueList.size()) return false;
            for (int i = 0; i < values.size(); i++) {
                if (SqlValueComparator.compare(values.get(i), valueList.get(i)) != 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 1;
            for (SqlValue v : values) {
                hash = 31 * hash + sqlValueHash(v);
            }
            return hash;
        }

        private static int sqlValueHash(SqlValue v) {
            return switch (v) {
                case SqlNull ignored -> 0;
                case SqlNumber n -> Double.hashCode(n.doubleValue());
                case SqlString(var value) -> value.hashCode();
                case SqlBoolean(var value) -> Boolean.hashCode(value);
                case SqlDate(var value) -> value.hashCode();
                case SqlDateTime(var value) -> value.hashCode();
            };
        }
    }
}
