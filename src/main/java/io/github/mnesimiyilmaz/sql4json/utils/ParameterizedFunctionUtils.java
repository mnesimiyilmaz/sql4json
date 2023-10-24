package io.github.mnesimiyilmaz.sql4json.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.BiFunction;

/**
 * @author mnesimiyilmaz
 */
public final class ParameterizedFunctionUtils {
    private static final Map<String, BiFunction<Object, List<Object>, Object>> FUNCTION_MAP;

    private ParameterizedFunctionUtils() {}

    public static Map<String, BiFunction<Object, List<Object>, Object>> getFunctionMap() {
        return FUNCTION_MAP;
    }

    public static BiFunction<Object, List<Object>, Object> getFunction(String functionName) {
        return FUNCTION_MAP.get(functionName);
    }

    static {
        Map<String, BiFunction<Object, List<Object>, Object>> functionMap = new HashMap<>();
        functionMap.put("to_date", (v, args) -> {
            if (v == null) return null;
            String dateString = (String) v;
            if (args == null || args.isEmpty()) {
                try {
                    return LocalDateTime.parse(dateString, DateTimeFormatter.ISO_DATE_TIME);
                } catch (DateTimeParseException e) {
                    return LocalDate.parse(dateString, DateTimeFormatter.ISO_DATE);
                }
            } else {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern((String) args.get(0));
                try {
                    return LocalDateTime.parse(dateString, formatter);
                } catch (DateTimeParseException e) {
                    return LocalDate.parse(dateString, formatter);
                }
            }
        });
        functionMap.put("upper", (v, args) -> v != null ? ((String) v).toUpperCase(getOrDefaultLocale(args)) : null);
        functionMap.put("lower", (v, args) -> v != null ? ((String) v).toLowerCase(getOrDefaultLocale(args)) : null);
        functionMap.put("coalesce", (v, args) -> v != null ? v : args.get(0));
        FUNCTION_MAP = Collections.unmodifiableMap(functionMap);
    }

    private static Locale getOrDefaultLocale(List<Object> args) {
        if (args == null || args.isEmpty()) {
            return Locale.getDefault();
        }
        return Locale.forLanguageTag((String) args.get(0));
    }

}
