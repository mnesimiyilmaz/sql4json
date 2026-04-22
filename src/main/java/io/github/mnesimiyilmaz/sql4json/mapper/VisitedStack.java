package io.github.mnesimiyilmaz.sql4json.mapper;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Per-{@code map()}-call cycle detector. Tracks identities currently on the mapping
 * stack, scoped to POJO targets (records can't cycle because Java prevents
 * self-reference in record component types). The identity key is any object whose
 * reference uniquely represents the frame — typically the underlying field map of a
 * {@code JsonObjectValue}.
 *
 * <p>Not thread-safe — one instance per top-level {@code map()} call.
 */
final class VisitedStack {

    private final Map<Object, Class<?>> stack = new IdentityHashMap<>();

    /**
     * @return {@code true} if the pair ({@code value}, {@code type}) is already on the stack
     */
    boolean contains(Object value, Class<?> type) {
        Class<?> existing = stack.get(value);
        return existing != null && existing.equals(type);
    }

    /**
     * Push a ({@code value}, {@code type}) frame. Caller must pop in {@code finally}.
     */
    void push(Object value, Class<?> type) {
        stack.put(value, type);
    }

    void pop(Object value) {
        stack.remove(value);
    }
}
