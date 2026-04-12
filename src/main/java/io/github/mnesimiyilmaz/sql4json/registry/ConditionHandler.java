package io.github.mnesimiyilmaz.sql4json.registry;

/**
 * Strategy interface for converting a ConditionContext into a CriteriaNode.
 * <p>
 * Implementations are registered in ConditionHandlerRegistry.
 * Each handler declares which condition types it can handle via canHandle().
 */
public interface ConditionHandler {
    /**
     * Returns {@code true} if this handler can process the given condition type.
     *
     * @param ctx the condition context to check
     * @return {@code true} if this handler applies
     */
    boolean canHandle(ConditionContext ctx);

    /**
     * Converts the given condition context into an executable criteria node.
     *
     * @param ctx       the condition context with all parameters
     * @param operators operator registry for comparison predicates
     * @param functions function registry for expression evaluation
     * @return a criteria node that evaluates this condition against rows
     */
    CriteriaNode handle(ConditionContext ctx, OperatorRegistry operators, FunctionRegistry functions);
}
