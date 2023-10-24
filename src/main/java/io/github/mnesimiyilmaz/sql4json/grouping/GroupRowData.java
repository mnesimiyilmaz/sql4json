package io.github.mnesimiyilmaz.sql4json.grouping;

import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.github.mnesimiyilmaz.sql4json.utils.MurmurHash3.hash32x86;
import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author mnesimiyilmaz
 */
@Getter
@ToString
public class GroupRowData {

    private final Map<String, Object> columnNameValuePairs;
    private final int                 hash;

    public GroupRowData(Map<String, Object> columnNameValuePairs) {
        Objects.requireNonNull(columnNameValuePairs);
        this.columnNameValuePairs = new HashMap<>(columnNameValuePairs);
        if (this.columnNameValuePairs.isEmpty()) {
            this.hash = 0;
        } else if (this.columnNameValuePairs.size() == 1) {
            this.hash = hash32x86(valueOf(columnNameValuePairs.values().iterator().next()).getBytes(UTF_8));
        } else {
            StringBuilder sb = new StringBuilder();
            for (Object value : columnNameValuePairs.values()) {
                sb.append(value);
            }
            this.hash = hash32x86(sb.toString().getBytes(UTF_8));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return this.hash == ((GroupRowData) o).getHash();
    }

    @Override
    public int hashCode() {
        return this.hash;
    }

}
