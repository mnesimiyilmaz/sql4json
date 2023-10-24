package io.github.mnesimiyilmaz.sql4json.sorting;

import io.github.mnesimiyilmaz.sql4json.definitions.OrderByColumnDefinion;
import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;
import io.github.mnesimiyilmaz.sql4json.utils.OrderByDirection;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author mnesimiyilmaz
 */
public class SortProcessor {

    private final List<Map<FieldKey, Object>> flattenedData;
    private final List<OrderByColumnDefinion> orderByColumns;

    public SortProcessor(List<Map<FieldKey, Object>> flattenedData,
                         List<OrderByColumnDefinion> orderByColumns) {
        this.flattenedData = flattenedData;
        this.orderByColumns = orderByColumns;
    }

    public Comparator<Map<FieldKey, Object>> buildComparator() {
        List<Comparator<Map<FieldKey, Object>>> comparators = new ArrayList<>();
        for (OrderByColumnDefinion orderByColumn : orderByColumns) {
            FieldKey fieldKey = FieldKey.of(orderByColumn.getColumnDefinion().getColumnName());
            flattenedData.stream()
                    .filter(x -> x.get(fieldKey) != null)
                    .findFirst()
                    .ifPresent(x -> comparators.add((o1, o2) -> {
                        Comparator<Object> comparator = getCompartorByValue(x.get(fieldKey), orderByColumn);
                        if (orderByColumn.getDirection().equals(OrderByDirection.DESC)) {
                            comparator = comparator.reversed();
                        }
                        return comparator.compare(o1.get(fieldKey), o2.get(fieldKey));
                    }));
        }
        Comparator<Map<FieldKey, Object>> result = null;
        for (Comparator<Map<FieldKey, Object>> comparator : comparators) {
            if (result == null) {
                result = comparator;
            } else {
                result.thenComparing(comparator);
            }
        }
        return result;
    }

    private Comparator<Object> getCompartorByValue(Object value, OrderByColumnDefinion columnDefinion) {
        if (columnDefinion.getColumnDefinion().getValueDecorator().isPresent()) {
            value = columnDefinion.getColumnDefinion().getValueDecorator().get().apply(value);
        }
        if (value instanceof Number) {
            return Comparator.comparingDouble(n -> ((Number) n).doubleValue());
        } else if (value instanceof String) {
            return Comparator.comparing(s -> (String) s);
        } else if (value instanceof Boolean) {
            return Comparator.comparing(s -> (Boolean) s);
        } else if (value instanceof LocalDate) {
            return Comparator.comparing(s -> (LocalDate) s);
        } else if (value instanceof LocalDateTime) {
            return Comparator.comparing(s -> (LocalDateTime) s);
        } else {
            return Comparator.comparing(Object::toString);
        }
    }

}
