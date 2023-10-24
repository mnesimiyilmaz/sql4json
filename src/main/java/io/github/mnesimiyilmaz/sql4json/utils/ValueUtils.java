package io.github.mnesimiyilmaz.sql4json.utils;

import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author mnesimiyilmaz
 */
public final class ValueUtils {

    private static final String                                                        NULL_CONTEXT_VALUE = "&null&";
    private static final List<Function<SQL4JsonParser.ValueContext, Optional<Object>>> VALUE_CONTEXTS;

    private ValueUtils() {}

    public static Object getValueFromContext(SQL4JsonParser.ValueWithNonAggFunctionContext valueContext) {
        Object value = getValueFromContext(valueContext.value());
        if (valueContext.NON_AGG_FUNCTION() != null) {
            List<Object> args = new ArrayList<>();
            if (valueContext.params() != null && valueContext.params().value() != null) {
                List<SQL4JsonParser.ValueContext> valueContexts = valueContext.params().value();
                for (SQL4JsonParser.ValueContext vc : valueContexts) {
                    args.add(ValueUtils.getValueFromContext(vc));
                }
            }
            value = ParameterizedFunctionUtils.getFunction(
                    valueContext.NON_AGG_FUNCTION().getText().toLowerCase()).apply(value, args);
        }
        return value;
    }

    public static Object getValueFromContext(SQL4JsonParser.ValueContext valueContext) {
        for (Function<SQL4JsonParser.ValueContext, Optional<Object>> conditionContext : VALUE_CONTEXTS) {
            Optional<Object> result = conditionContext.apply(valueContext);
            if (result.isPresent()) {
                return NULL_CONTEXT_VALUE.equals(result.get()) ? null : result.get();
            }
        }
        throw new IllegalStateException("Value is not supported " + valueContext.getText());
    }

    static {
        List<Function<SQL4JsonParser.ValueContext, Optional<Object>>> contextList = new ArrayList<>();
        contextList.add(c -> {
            if (c.STRING() != null) {
                String str = c.getText();
                return Optional.of(str.substring(1, str.length() - 1));
            }
            return Optional.empty();
        });
        contextList.add(c -> {
            if (c.BOOLEAN() != null) {
                return Optional.of(Boolean.parseBoolean(c.getText().toLowerCase()));
            }
            return Optional.empty();
        });
        contextList.add(c -> {
            if (c.NUMBER() != null) {
                return Optional.of(Double.parseDouble(c.getText()));
            }
            return Optional.empty();
        });
        contextList.add(c -> {
            if (c.NULL() != null) {
                return Optional.of(NULL_CONTEXT_VALUE);
            }
            return Optional.empty();
        });
        contextList.add(c -> {
            if (c.VALUE_FUNCTION() != null) {
                return Optional.of(ValueFunctionUtils.getValueFunction(c.getText().toLowerCase()).apply(null));
            }
            return Optional.empty();
        });
        VALUE_CONTEXTS = Collections.unmodifiableList(contextList);
    }

}
