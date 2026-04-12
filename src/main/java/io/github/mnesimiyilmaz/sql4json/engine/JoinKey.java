package io.github.mnesimiyilmaz.sql4json.engine;

import io.github.mnesimiyilmaz.sql4json.types.SqlNumber;
import io.github.mnesimiyilmaz.sql4json.types.SqlValue;

import java.util.List;

/**
 * Composite key for hash JOIN lookups.
 * Normalizes numeric values to double so that integer 1 matches double 1.0.
 */
record JoinKey(List<SqlValue> values) {

    JoinKey {
        values = List.copyOf(values);
    }

    /**
     * Factory that normalizes SqlNumber values to double before storing.
     */
    static JoinKey of(List<SqlValue> raw) {
        var normalized = raw.stream()
                .map(v -> v instanceof SqlNumber n ? SqlNumber.of(n.doubleValue()) : v)
                .toList();
        return new JoinKey(normalized);
    }
}
