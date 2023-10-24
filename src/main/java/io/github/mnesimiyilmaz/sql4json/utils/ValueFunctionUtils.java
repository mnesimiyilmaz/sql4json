package io.github.mnesimiyilmaz.sql4json.utils;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author mnesimiyilmaz
 */
public final class ValueFunctionUtils {

    private static final Map<String, Function<List<Object>, Object>> VALUE_FUNCTION_MAP;

    private ValueFunctionUtils() {}

    public static Map<String, Function<List<Object>, Object>> getValueFunctionMap() {
        return VALUE_FUNCTION_MAP;
    }

    public static Function<List<Object>, Object> getValueFunction(String functionName) {
        return VALUE_FUNCTION_MAP.get(functionName);
    }

    static {
        VALUE_FUNCTION_MAP = Collections.singletonMap("now()", args -> LocalDateTime.now());
    }

}
