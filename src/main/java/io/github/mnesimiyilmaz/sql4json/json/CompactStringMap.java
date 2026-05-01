package io.github.mnesimiyilmaz.sql4json.json;

import java.util.*;

/**
 * A compact, immutable, insertion-ordered {@code Map<String, V>} backed by parallel arrays.
 * Designed for JSON objects where field counts are small (typically 5-20 keys).
 *
 * <p>Memory overhead is ~3x less than {@link LinkedHashMap} because there are no
 * Entry/Node objects, no linked-list pointers, and no hash-table backing array.
 *
 * <p>Lookup is O(n) via linear scan, but for small maps this is faster than HashMap
 * due to cache-line locality — all keys are contiguous in memory.
 *
 * <p>This map is unmodifiable: all mutation methods throw {@link UnsupportedOperationException}.
 */
// NullableProblems: JDK's Map/AbstractMap contract annotates return types with @NotNull; this
//                   library avoids a nullability-annotation dependency, so overrides inherit
//                   the contract implicitly (none of the overridden methods return null).
@SuppressWarnings("NullableProblems")
final class CompactStringMap<V> extends AbstractMap<String, V> {

    private final String[] keys;
    @SuppressWarnings("java:S2387")
    private final Object[] values;
    private final int      size;

    CompactStringMap(LinkedHashMap<String, V> source) {
        int n = source.size();
        keys = new String[n];
        values = new Object[n];
        size = n;
        int i = 0;
        for (var entry : source.entrySet()) {
            keys[i] = entry.getKey();
            values[i] = entry.getValue();
            i++;
        }
    }

    /**
     * Wraps {@code keys} and {@code values} arrays directly. Caller transfers
     * ownership — the arrays must not be mutated after construction. Used by
     * {@link JsonParser} to skip the intermediate {@link LinkedHashMap} that
     * the legacy constructor required.
     *
     * <p>Uses {@code keys.length} as the size — the arrays must be exact-fit.</p>
     *
     * @param keys   the key array; length must match {@code values.length}
     * @param values the value array
     */
    CompactStringMap(String[] keys, Object[] values) {
        this(keys, values, keys.length);
    }

    /**
     * Wraps {@code keys} and {@code values} arrays with an explicit size when
     * the arrays may have trailing unused slots (e.g. parser builders that
     * skip trim allocations for nearly-full buffers).
     *
     * @param keys   the key array (capacity ≥ {@code size})
     * @param values the value array (capacity ≥ {@code size})
     * @param size   logical entry count
     */
    CompactStringMap(String[] keys, Object[] values, int size) {
        this.keys = keys;
        this.values = values;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return indexOf(key) >= 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        int idx = indexOf(key);
        return idx >= 0 ? (V) values[idx] : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getOrDefault(Object key, V defaultValue) {
        int idx = indexOf(key);
        return idx >= 0 ? (V) values[idx] : defaultValue;
    }

    private int indexOf(Object key) {
        if (key instanceof String s) {
            for (int i = 0; i < size; i++) {
                if (keys[i].equals(s)) return i;
            }
        }
        return -1;
    }

    @Override
    public Set<Entry<String, V>> entrySet() {
        return new EntrySet();
    }

    @Override
    public Set<String> keySet() {
        return new KeySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Map<?, ?> other)) return false;
        if (other.size() != size) return false;
        for (int i = 0; i < size; i++) {
            if (!Objects.equals(values[i], other.get(keys[i]))) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (int i = 0; i < size; i++) {
            h += Objects.hashCode(keys[i]) ^ Objects.hashCode(values[i]);
        }
        return h;
    }

    private final class KeySet extends AbstractSet<String> {
        @Override
        public int size() {
            return size;
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < size;
                }

                @Override
                public String next() {
                    if (i >= size) throw new NoSuchElementException();
                    return keys[i++];
                }
            };
        }

        @Override
        public boolean contains(Object o) {
            return containsKey(o);
        }
    }

    private final class EntrySet extends AbstractSet<Entry<String, V>> {
        @Override
        public int size() {
            return size;
        }

        @Override
        public Iterator<Entry<String, V>> iterator() {
            return new Iterator<>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < size;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Entry<String, V> next() {
                    if (i >= size) throw new NoSuchElementException();
                    int idx = i++;
                    return new AbstractMap.SimpleImmutableEntry<>(keys[idx], (V) values[idx]);
                }
            };
        }
    }
}
