package io.github.mnesimiyilmaz.sql4json.json;

import io.github.mnesimiyilmaz.sql4json.engine.RowAccessor;
import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.parser.SelectColumnDef;
import io.github.mnesimiyilmaz.sql4json.registry.FunctionRegistry;
import io.github.mnesimiyilmaz.sql4json.types.JsonValue;

import java.util.List;
import java.util.stream.Stream;

/**
 * Serializes a stream of rows into a JSON array string.
 * Each row is unflattened, serialized, then discarded — O(1) per row.
 */
public final class StreamingSerializer {

    private StreamingSerializer() {
    }

    /**
     * Serialize a stream of rows into a JSON array string.
     * Each row is unflattened and serialized inline, then discarded.
     *
     * @param rows             sequential stream of rows
     * @param columns          SELECT column definitions for unflattening
     * @param functionRegistry function registry for expression evaluation
     * @param maxRows          maximum number of rows allowed; throws if exceeded
     * @return JSON array string
     * @throws SQL4JsonExecutionException if the row count would exceed {@code maxRows}
     */
    public static String serialize(Stream<RowAccessor> rows, List<SelectColumnDef> columns,
                                   FunctionRegistry functionRegistry, int maxRows) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        boolean[] first = {true};
        int[] count = {0};
        rows.forEach(row -> {
            if (count[0] >= maxRows) {
                throw new SQL4JsonExecutionException(
                        "STREAMING row count exceeds configured maximum (" + maxRows + ")");
            }
            count[0]++;
            if (!first[0]) sb.append(',');
            first[0] = false;
            JsonValue rowValue = JsonUnflattener.unflattenRow(row, columns, functionRegistry);
            JsonSerializer.writeTo(sb, rowValue);
        });
        sb.append(']');
        return sb.toString();
    }
}
