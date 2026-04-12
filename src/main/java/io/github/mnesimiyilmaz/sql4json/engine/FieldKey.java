package io.github.mnesimiyilmaz.sql4json.engine;

import java.util.HashMap;

/**
 * Immutable key for a flattened JSON field path. Tracks both the full key
 * (e.g. {@code "items[0].name"}) and its array-index-stripped family
 * (e.g. {@code "items.name"}) for nested field grouping during unflattening.
 */
public final class FieldKey {

    private final String key;
    private final String family;
    private final int    hashCode; // Pre-computed for performance

    /**
     * Query-scoped interner — created once per query execution, GC'd when query finishes.
     * Sharing identical path strings and FieldKey instances across rows reduces GC pressure.
     */
    public static final class Interner {
        private final HashMap<String, String>   stringPool   = new HashMap<>();
        private final HashMap<String, FieldKey> fieldKeyPool = new HashMap<>();

        /**
         * Creates a new empty interner.
         */
        public Interner() {
        }

        /**
         * Interns a string, returning the canonical instance.
         *
         * @param s the string to intern
         * @return the canonical instance of the string
         */
        public String intern(String s) {
            return stringPool.computeIfAbsent(s, k -> k);
        }

        /**
         * Interns a FieldKey, returning the canonical instance for the given key path.
         *
         * @param key the field key path to intern
         * @return the canonical FieldKey instance for the given path
         */
        public FieldKey internFieldKey(String key) {
            return fieldKeyPool.computeIfAbsent(key, k -> {
                String internedKey = intern(k);
                String derivedFamily = stripArrayIndices(internedKey);
                String internedFamily = derivedFamily.equals(internedKey) ? internedKey : intern(derivedFamily);
                return new FieldKey(internedKey, internedFamily);
            });
        }
    }

    /**
     * Primary factory — uses interner for string and FieldKey instance sharing.
     *
     * @param key      the field key path
     * @param interner the interner to use for deduplication
     * @return the interned FieldKey instance
     */
    public static FieldKey of(String key, Interner interner) {
        return interner.internFieldKey(key);
    }

    /**
     * Convenience factory for tests / one-off usage (no interning).
     *
     * @param key the field key path
     * @return a new FieldKey instance
     */
    public static FieldKey of(String key) {
        String family = stripArrayIndices(key);
        return new FieldKey(key, family);
    }

    /**
     * Strips array index brackets from a field path.
     * "items[0].name" → "items.name", "matrix[0][1]" → "matrix"
     * Returns the input string unchanged (same reference) if no indices found.
     * Only called with paths produced by JsonFlattener, which always generates well-formed [N] indices.
     */
    static String stripArrayIndices(String path) {
        int bracketIdx = path.indexOf('[');
        if (bracketIdx < 0) return path;

        StringBuilder sb = new StringBuilder(path.length());
        int i = 0;
        while (i < path.length()) {
            char c = path.charAt(i);
            if (c == '[') {
                do i++;
                while (i < path.length() && path.charAt(i) != ']');
            } else {
                sb.append(c);
            }
            i++; // skip ']'
        }
        return sb.toString();
    }

    private FieldKey(String key, String family) {
        this.key = key;
        this.family = family;
        this.hashCode = key.hashCode();
    }

    /**
     * Returns the full field path (e.g. {@code "items[0].name"}).
     *
     * @return the full field path
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the array-index-stripped family path (e.g. {@code "items.name"}).
     *
     * @return the family path with array indices removed
     */
    public String getFamily() {
        return family;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FieldKey fk)) return false;
        return key.equals(fk.key);
    }

    @Override
    public String toString() {
        return "FieldKey{" + key + "}";
    }
}
