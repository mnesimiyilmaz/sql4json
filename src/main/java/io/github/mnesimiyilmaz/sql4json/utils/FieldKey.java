package io.github.mnesimiyilmaz.sql4json.utils;

import lombok.Getter;

import java.util.Objects;

/**
 * @author mnesimiyilmaz
 */
@Getter
public class FieldKey {

    private final String key;
    private final String family;

    public FieldKey(String key, String family) {
        Objects.requireNonNull(key);
        this.key = key;
        this.family = family;
    }

    public FieldKey(String key) {
        this(key, null);
    }

    public static FieldKey of(String key, String family) {
        return new FieldKey(key, family);
    }

    public static FieldKey of(String key) {
        return new FieldKey(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldKey fieldKey = (FieldKey) o;
        return Objects.equals(key, fieldKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

}
