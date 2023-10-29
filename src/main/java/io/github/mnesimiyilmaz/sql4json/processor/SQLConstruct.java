package io.github.mnesimiyilmaz.sql4json.processor;

import io.github.mnesimiyilmaz.sql4json.condition.CriteriaNode;
import io.github.mnesimiyilmaz.sql4json.definitions.JsonColumnWithNonAggFunctionDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.OrderByColumnDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.SelectColumnDefinition;
import io.github.mnesimiyilmaz.sql4json.grouping.GroupByInput;
import io.github.mnesimiyilmaz.sql4json.grouping.GroupByProcessor;
import io.github.mnesimiyilmaz.sql4json.sorting.SortProcessor;
import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class SQLConstruct {
    private List<SelectColumnDefinition>               selectedColumns;
    private CriteriaNode                               whereClause;
    private List<JsonColumnWithNonAggFunctionDefinion> groupByColumns;
    private CriteriaNode                               havingClause;
    private List<OrderByColumnDefinion>                orderByColumns;

    public static SQLConstruct newInstance(SQLBuilder builder) {
        SQLConstruct sqlConstruct = new SQLConstruct();
        sqlConstruct.selectedColumns = builder.getSelectedColumns();
        sqlConstruct.whereClause = builder.getWhereClause();
        sqlConstruct.groupByColumns = builder.getGroupByColumns();
        sqlConstruct.havingClause = builder.getHavingClause();
        sqlConstruct.orderByColumns = builder.getOrderByColumns();
        return sqlConstruct;
    }

    public List<Map<FieldKey, Object>> apply(List<Map<FieldKey, Object>> flattenedData) {
        if (whereClause != null) {
            flattenedData.removeIf(x -> !whereClause.test(x));
        }
        if (groupByColumns != null) {
            flattenedData = new GroupByProcessor(new GroupByInput(
                    flattenedData, selectedColumns, groupByColumns, havingClause)).process();
        }
        if (orderByColumns != null) {
            flattenedData.sort(new SortProcessor(flattenedData, orderByColumns).buildComparator());
        }
        if (groupByColumns == null && !selectedColumns.get(0).isAsterisk()) {
            flattenedData = flattenedData.stream().map(this::getSelectedRowData).collect(Collectors.toList());
        }
        return flattenedData;
    }

    private Map<FieldKey, Object> getSelectedRowData(Map<FieldKey, Object> row) {
        Map<FieldKey, Object> result = new HashMap<>();
        for (SelectColumnDefinition selectedColumn : selectedColumns) {
            FieldKey fieldKey = FieldKey.of(selectedColumn.getColumnDefinition().getColumnDefinition().getColumnName());
            if (isObjectField(row, fieldKey.getKey())) {
                result.putAll(getObjectFieldValues(row, fieldKey.getKey(), selectedColumn.getAlias()));
            } else {
                Object value = row.get(fieldKey);
                Optional<Function<Object, Object>> decorator = selectedColumn.getColumnDefinition().getColumnDefinition().getValueDecorator();
                if (decorator.isPresent()) {
                    value = decorator.get().apply(value);
                }
                if (selectedColumn.getAlias() != null) {
                    result.put(FieldKey.of(selectedColumn.getAlias()), value);
                } else {
                    result.put(fieldKey, value);
                }
            }
        }
        return result;
    }

    private boolean isObjectField(Map<FieldKey, Object> row, String selectPath) {
        return row.keySet().stream().filter(x -> x.getKey().startsWith(selectPath)).count() > 1L;
    }

    private Map<FieldKey, Object> getObjectFieldValues(Map<FieldKey, Object> row, String selectPath, String alias) {
        if (alias == null) {
            return row.entrySet().stream()
                    .filter(x -> x.getKey().getKey().startsWith(selectPath))
                    .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
        } else {
            Map<FieldKey, Object> result = new HashMap<>();
            for (Map.Entry<FieldKey, Object> entry : row.entrySet()) {
                if (entry.getKey().getKey().startsWith(selectPath)) {
                    result.put(FieldKey.of(alias + entry.getKey().getKey().substring(selectPath.length())), entry.getValue());
                }
            }
            return result;
        }
    }

}
