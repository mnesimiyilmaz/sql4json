package io.github.mnesimiyilmaz.sql4json.registry;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonExecutionException;
import io.github.mnesimiyilmaz.sql4json.settings.Sql4jsonSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Registry of {@link ConditionHandler} implementations. Resolves a {@link ConditionContext}
 * to an executable {@link CriteriaNode} by delegating to the first matching handler.
 *
 * <p>Instances are cached per distinct {@code likePatternCacheSize} via {@link #forSettings}.
 */
public final class ConditionHandlerRegistry {

    private List<ConditionHandler> handlers = new ArrayList<>();

    private final OperatorRegistry operatorRegistry;
    private final FunctionRegistry functionRegistry;

    /**
     * Creates a registry with the given operator and function registries.
     *
     * @param ops operator registry for comparison predicates
     * @param fns function registry for expression evaluation
     */
    public ConditionHandlerRegistry(OperatorRegistry ops, FunctionRegistry fns) {
        this.operatorRegistry = ops;
        this.functionRegistry = fns;
    }

    /**
     * Registers a new condition handler. Must be called before {@link #freeze}.
     *
     * @param handler the condition handler to register
     */
    public void register(ConditionHandler handler) {
        handlers.add(handler);
    }

    /**
     * Makes the handler list unmodifiable. Called after all handlers are registered.
     */
    public void freeze() {
        handlers = Collections.unmodifiableList(handlers);
    }

    /**
     * Resolves a condition context to an executable criteria node.
     *
     * @param ctx the condition context to resolve
     * @return the executable criteria node for the given context
     */
    public CriteriaNode resolve(ConditionContext ctx) {
        return handlers.stream()
                .filter(h -> h.canHandle(ctx))
                .findFirst()
                .orElseThrow(() -> new SQL4JsonExecutionException(
                        "No handler found for condition type: " + ctx.type()))
                .handle(ctx, operatorRegistry, functionRegistry);
    }

    // One registry per distinct likePatternCacheSize. The registry is a pure
    // function of that one int, so keying on the size gives maximum sharing
    // and bounds this map to O(distinct sizes) — in practice 1, since nearly
    // every caller uses defaults(). Keying on the full Sql4jsonSettings record
    // would let unrelated field changes (LimitsSettings / SecuritySettings)
    // accumulate unbounded entries, reintroducing a DoS vector.
    private static final ConcurrentMap<Integer, ConditionHandlerRegistry> CACHE =
            new ConcurrentHashMap<>();

    /**
     * Returns a cached registry for the given settings (keyed by likePatternCacheSize).
     *
     * @param settings the settings whose cache configuration determines the registry instance
     * @return a cached or newly created registry for the given settings
     */
    public static ConditionHandlerRegistry forSettings(Sql4jsonSettings settings) {
        Objects.requireNonNull(settings, "settings");
        return CACHE.computeIfAbsent(
                settings.cache().likePatternCacheSize(),
                ConditionHandlerRegistry::build);
    }

    private static ConditionHandlerRegistry build(int likePatternCacheSize) {
        var ops = OperatorRegistry.getDefault();
        var fns = FunctionRegistry.getDefault();
        var patternCache = new BoundedPatternCache(likePatternCacheSize);
        var r = new ConditionHandlerRegistry(ops, fns);
        r.register(new ComparisonConditionHandler());
        r.register(new LikeConditionHandler(patternCache));
        r.register(new NullCheckConditionHandler());
        r.register(new InConditionHandler());
        r.register(new BetweenConditionHandler());
        r.register(new NotLikeConditionHandler(patternCache));
        r.register(new ArrayPredicateConditionHandler());
        r.freeze();
        return r;
    }
}
