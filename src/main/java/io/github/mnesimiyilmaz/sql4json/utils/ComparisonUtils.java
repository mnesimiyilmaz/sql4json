package io.github.mnesimiyilmaz.sql4json.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;

import static io.github.mnesimiyilmaz.sql4json.utils.ComparisonOperator.*;

/**
 * @author mnesimiyilmaz
 */
public final class ComparisonUtils {

    private static final Map<String, BiPredicate<Object, Object>> COMPARISON_OPERATOR_MAP;

    private ComparisonUtils() {}

    public static Map<String, BiPredicate<Object, Object>> getComparisonOperatorMap() {
        return COMPARISON_OPERATOR_MAP;
    }

    public static BiPredicate<Object, Object> getComparisonPredicate(String operator) {
        return COMPARISON_OPERATOR_MAP.get(operator);
    }

    static {
        Map<String, BiPredicate<Object, Object>> comparisonOperatorMap = new HashMap<>();
        comparisonOperatorMap.put(GT.getExp(), (x, y) -> {
            if (x == null) return false;
            if (x instanceof Number) {
                return ((Number) x).doubleValue() > ((Number) y).doubleValue();
            } else if (x instanceof LocalDate) {
                return ((LocalDate) x).isAfter((LocalDate) y);
            } else if (x instanceof LocalDateTime) {
                return ((LocalDateTime) x).isAfter((LocalDateTime) y);
            } else {
                return x.toString().compareTo(y.toString()) > 0;
            }
        });
        comparisonOperatorMap.put(GTE.getExp(), (x, y) -> {
            if (x == null) return false;
            if (x instanceof Number) {
                return ((Number) x).doubleValue() >= ((Number) y).doubleValue();
            } else if (x instanceof LocalDate) {
                return ((LocalDate) x).isAfter((LocalDate) y) || ((LocalDate) x).isEqual((LocalDate) y);
            } else if (x instanceof LocalDateTime) {
                return ((LocalDateTime) x).isAfter((LocalDateTime) y) || ((LocalDateTime) x).isEqual((LocalDateTime) y);
            } else {
                return x.toString().compareTo(y.toString()) >= 0;
            }
        });
        comparisonOperatorMap.put(LT.getExp(), (x, y) -> {
            if (x == null) return false;
            if (x instanceof Number) {
                return ((Number) x).doubleValue() < ((Number) y).doubleValue();
            } else if (x instanceof LocalDate) {
                return ((LocalDate) x).isBefore((LocalDate) y);
            } else if (x instanceof LocalDateTime) {
                return ((LocalDateTime) x).isBefore((LocalDateTime) y);
            } else {
                return x.toString().compareTo(y.toString()) < 0;
            }
        });
        comparisonOperatorMap.put(LTE.getExp(), (x, y) -> {
            if (x == null) return false;
            if (x instanceof Number) {
                return ((Number) x).doubleValue() <= ((Number) y).doubleValue();
            } else if (x instanceof LocalDate) {
                return ((LocalDate) x).isBefore((LocalDate) y) || ((LocalDate) x).isEqual((LocalDate) y);
            } else if (x instanceof LocalDateTime) {
                return ((LocalDateTime) x).isBefore((LocalDateTime) y) || ((LocalDateTime) x).isEqual((LocalDateTime) y);
            } else {
                return x.toString().compareTo(y.toString()) <= 0;
            }
        });
        comparisonOperatorMap.put(EQ.getExp(), (x, y) -> {
            if (y == null) return x == null;
            if (x == null) return false;
            if (x instanceof LocalDate) {
                return ((LocalDate) x).isEqual((LocalDate) y);
            } else if (x instanceof LocalDateTime) {
                return ((LocalDateTime) x).isEqual((LocalDateTime) y);
            } else {
                return Objects.equals(x, y);
            }
        });
        comparisonOperatorMap.put(NE.getExp(), (x, y) -> {
            if (y == null) return x != null;
            if (x instanceof LocalDate) {
                return !((LocalDate) x).isEqual((LocalDate) y);
            } else if (x instanceof LocalDateTime) {
                return !((LocalDateTime) x).isEqual((LocalDateTime) y);
            } else {
                return !Objects.equals(x, y);
            }
        });
        COMPARISON_OPERATOR_MAP = Collections.unmodifiableMap(comparisonOperatorMap);
    }

}
