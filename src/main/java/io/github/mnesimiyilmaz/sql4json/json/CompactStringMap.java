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
final class CompactStringMap<V> extends AbstractMap<String, V> {

    private final String[] keys;
    @SuppressWarnings("java:S2387")
    private final Object[] values;

    CompactStringMap(LinkedHashMap<String, V> source) {
        int size = source.size();
        keys = new String[size];
        values = new Object[size];
        int i = 0;
        for (var entry : source.entrySet()) {
            keys[i] = entry.getKey();
            values[i] = entry.getValue();
            i++;
        }
    }

    @Override
    public int size() {
        return keys.length;
    }

    @Override
    public boolean isEmpty() {
        return keys.length == 0;
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
            for (int i = 0; i < keys.length; i++) {
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
        if (other.size() != keys.length) return false;
        for (int i = 0; i < keys.length; i++) {
            if (!Objects.equals(values[i], other.get(keys[i]))) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (int i = 0; i < keys.length; i++) {
            h += Objects.hashCode(keys[i]) ^ Objects.hashCode(values[i]);
        }
        return h;
    }

    private final class KeySet extends AbstractSet<String> {
        @Override
        public int size() {
            return keys.length;
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < keys.length;
                }

                @Override
                public String next() {
                    if (i >= keys.length) throw new NoSuchElementException();
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
            return keys.length;
        }

        @Override
        public Iterator<Entry<String, V>> iterator() {
            return new Iterator<>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < keys.length;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Entry<String, V> next() {
                    if (i >= keys.length) throw new NoSuchElementException();
                    int idx = i++;
                    return new AbstractMap.SimpleImmutableEntry<>(keys[idx], (V) values[idx]);
                }
            };
        }
    }
}
