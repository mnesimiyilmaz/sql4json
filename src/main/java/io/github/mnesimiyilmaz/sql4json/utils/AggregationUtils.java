package io.github.mnesimiyilmaz.sql4json.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author mnesimiyilmaz
 */
public final class AggregationUtils {

    private static final Map<AggregateFunction, Function<List<Object>, Object>> AGG_MAP;

    private AggregationUtils() {}

    public static Map<AggregateFunction, Function<List<Object>, Object>> getAggregationFunctionMap() {
        return AGG_MAP;
    }

    public static Function<List<Object>, Object> getAggregationFunction(AggregateFunction function) {
        return AGG_MAP.get(function);
    }

    static {
        Map<AggregateFunction, Function<List<Object>, Object>> aggMap = new EnumMap<>(AggregateFunction.class);
        aggMap.put(AggregateFunction.COUNT, List::size);
        aggMap.put(AggregateFunction.SUM, values -> values.stream().map(x -> ((Number) x).doubleValue()).reduce(0.0, Double::sum));
        aggMap.put(AggregateFunction.AVG, values -> values.stream().map(x -> ((Number) x).doubleValue()).reduce(0.0, Double::sum) / values.size());
        aggMap.put(AggregateFunction.MIN, values -> {
            if (values.get(0) instanceof Number) {
                return values.stream().map(x -> ((Number) x).doubleValue()).min(Double::compareTo).orElse(null);
            } else if (values.get(0) instanceof LocalDate) {
                return values.stream().map(x -> ((LocalDate) x)).min(LocalDate::compareTo).orElse(null);
            } else if (values.get(0) instanceof LocalDateTime) {
                return values.stream().map(x -> ((LocalDateTime) x)).min(LocalDateTime::compareTo).orElse(null);
            } else {
                throw new IllegalArgumentException("Unsupported type for min aggregation: " + values.get(0).getClass().getName());
            }
        });
        aggMap.put(AggregateFunction.MAX, values -> {
            if (values.get(0) instanceof Number) {
                return values.stream().map(x -> ((Number) x).doubleValue()).max(Double::compareTo).orElse(null);
            } else if (values.get(0) instanceof LocalDate) {
                return values.stream().map(x -> ((LocalDate) x)).max(LocalDate::compareTo).orElse(null);
            } else if (values.get(0) instanceof LocalDateTime) {
                return values.stream().map(x -> ((LocalDateTime) x)).max(LocalDateTime::compareTo).orElse(null);
            } else {
                throw new IllegalArgumentException("Unsupported type for max aggregation: " + values.get(0).getClass().getName());
            }
        });
        AGG_MAP = Collections.unmodifiableMap(aggMap);
    }

}
