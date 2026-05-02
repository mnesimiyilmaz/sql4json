// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonBindException;
import java.util.*;

/**
 * Immutable carrier of parameter bindings for {@link PreparedQuery} and {@link SQL4JsonEngine} parameterised execution.
 *
 * <p>Modes are mutually exclusive: an instance is either <b>named</b> (binds by {@code :name}) or <b>positional</b>
 * (binds by {@code 0,1,...} for {@code ?}). Attempting to mix produces {@link SQL4JsonBindException}. Every
 * {@code bind(...)} returns a new instance — thread-safe.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * // Named
 * BoundParameters p = BoundParameters.named()
 *     .bind("minAge", 25)
 *     .bind("dept", "Engineering");
 *
 * // Positional
 * BoundParameters p2 = BoundParameters.of(25, 50_000, 150_000);
 *
 * // From a map
 * BoundParameters p3 = BoundParameters.of(Map.of("min", 10, "max", 100));
 * }</pre>
 *
 * @see SQL4JsonBindException
 * @since 1.1.0
 */
public final class BoundParameters {

    private enum Mode {
        EMPTY,
        NAMED,
        POSITIONAL
    }

    /** Shared empty instance. Neither named nor positional — compatible with parameterless queries. */
    public static final BoundParameters EMPTY =
            new BoundParameters(Mode.EMPTY, Collections.emptyMap(), Collections.emptyList());

    private final Mode mode;
    private final Map<String, Object> named;
    private final List<Object> positional;

    private BoundParameters(Mode mode, Map<String, Object> named, List<Object> positional) {
        this.mode = mode;
        this.named = named;
        this.positional = positional;
    }

    /**
     * Returns an empty instance in named mode.
     *
     * @return a new empty named {@code BoundParameters}
     */
    public static BoundParameters named() {
        return new BoundParameters(Mode.NAMED, new LinkedHashMap<>(), Collections.emptyList());
    }

    /**
     * Returns an empty instance in positional mode.
     *
     * @return a new empty positional {@code BoundParameters}
     */
    public static BoundParameters positional() {
        return new BoundParameters(Mode.POSITIONAL, Collections.emptyMap(), new ArrayList<>());
    }

    /**
     * Returns a positional instance holding {@code values} in order.
     *
     * @param values the parameter values in positional order
     * @return a positional {@code BoundParameters} containing {@code values}
     */
    public static BoundParameters of(Object... values) {
        List<Object> list = new ArrayList<>(values.length);
        Collections.addAll(list, values);
        return new BoundParameters(Mode.POSITIONAL, Collections.emptyMap(), list);
    }

    /**
     * Returns a named instance holding the entries from {@code values}.
     *
     * @param values the named parameter map; iteration order is preserved when a {@link LinkedHashMap} is supplied
     * @return a named {@code BoundParameters} containing {@code values}
     */
    public static BoundParameters of(Map<String, ?> values) {
        Map<String, Object> m = new LinkedHashMap<>(values);
        return new BoundParameters(Mode.NAMED, m, Collections.emptyList());
    }

    /**
     * Returns a new instance with {@code name} bound to {@code value}.
     *
     * @param name the parameter name (without the leading {@code :})
     * @param value the parameter value (may be {@code null})
     * @return a new {@code BoundParameters} with the additional binding
     * @throws SQL4JsonBindException if this instance is in positional mode
     */
    public BoundParameters bind(String name, Object value) {
        if (mode == Mode.POSITIONAL) {
            throw new SQL4JsonBindException("Cannot mix named and positional bindings on the same BoundParameters");
        }
        Map<String, Object> copy = new LinkedHashMap<>(named);
        copy.put(name, value);
        return new BoundParameters(Mode.NAMED, copy, Collections.emptyList());
    }

    /**
     * Returns a new instance with position {@code index} bound to {@code value}. The internal list grows as needed to
     * accommodate the index.
     *
     * @param index the zero-based parameter index
     * @param value the parameter value (may be {@code null})
     * @return a new {@code BoundParameters} with the additional binding
     * @throws SQL4JsonBindException if this instance is in named mode or {@code index < 0}
     */
    public BoundParameters bind(int index, Object value) {
        if (mode == Mode.NAMED) {
            throw new SQL4JsonBindException("Cannot mix named and positional bindings on the same BoundParameters");
        }
        if (index < 0) {
            throw new SQL4JsonBindException("Parameter index must be non-negative, got " + index);
        }
        List<Object> copy = new ArrayList<>(positional);
        while (copy.size() <= index) {
            copy.add(null);
        }
        copy.set(index, value);
        return new BoundParameters(Mode.POSITIONAL, Collections.emptyMap(), copy);
    }

    /**
     * Batch positional bind: appends all {@code values} in order.
     *
     * @param values the values to append in positional order
     * @return a new {@code BoundParameters} with the appended bindings
     * @throws SQL4JsonBindException if this instance is in named mode
     */
    public BoundParameters bindAll(Object... values) {
        if (mode == Mode.NAMED) {
            throw new SQL4JsonBindException("Cannot mix named and positional bindings on the same BoundParameters");
        }
        List<Object> copy = new ArrayList<>(positional);
        Collections.addAll(copy, values);
        return new BoundParameters(Mode.POSITIONAL, Collections.emptyMap(), copy);
    }

    /**
     * Returns {@code true} iff this instance holds named parameters.
     *
     * @return {@code true} for named mode, {@code false} for positional or empty
     */
    public boolean isNamed() {
        return mode == Mode.NAMED;
    }

    /**
     * Returns {@code true} iff this instance holds zero bindings (includes {@link #EMPTY}).
     *
     * @return {@code true} when no parameters have been bound
     */
    public boolean isEmpty() {
        return (mode == Mode.EMPTY)
                || (mode == Mode.NAMED && named.isEmpty())
                || (mode == Mode.POSITIONAL && positional.isEmpty());
    }

    /**
     * Returns the number of positional values bound; {@code 0} if not in positional mode.
     *
     * @return the positional parameter count
     */
    public int positionalCount() {
        return positional.size();
    }

    /**
     * Returns the number of named values bound; {@code 0} if not in named mode.
     *
     * @return the named parameter count
     */
    public int namedCount() {
        return named.size();
    }

    /**
     * Returns the value bound at {@code index}.
     *
     * @param index the zero-based parameter index
     * @return the value bound at {@code index}
     * @throws SQL4JsonBindException if not in positional mode or if {@code index} is out of range
     */
    public Object getByIndex(int index) {
        if (mode != Mode.POSITIONAL) {
            throw new SQL4JsonBindException("BoundParameters is not in positional mode");
        }
        if (index < 0 || index >= positional.size()) {
            throw new SQL4JsonBindException("Parameter index " + index + " not bound");
        }
        return positional.get(index);
    }

    /**
     * Returns the value bound to {@code name}.
     *
     * @param name the parameter name (without the leading {@code :})
     * @return the value bound to {@code name}
     * @throws SQL4JsonBindException if not in named mode or if {@code name} has no binding
     */
    public Object getByName(String name) {
        if (mode != Mode.NAMED) {
            throw new SQL4JsonBindException("BoundParameters is not in named mode");
        }
        if (!named.containsKey(name)) {
            throw new SQL4JsonBindException("Missing parameter ':" + name + "'");
        }
        return named.get(name);
    }
}
