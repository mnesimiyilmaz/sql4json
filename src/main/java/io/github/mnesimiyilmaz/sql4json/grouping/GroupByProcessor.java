package io.github.mnesimiyilmaz.sql4json.grouping;

import io.github.mnesimiyilmaz.sql4json.definitions.JsonColumnWithNonAggFunctionDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.SelectColumnDefinition;
import io.github.mnesimiyilmaz.sql4json.utils.AggregateFunction;
import io.github.mnesimiyilmaz.sql4json.utils.AggregationUtils;
import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.mnesimiyilmaz.sql4json.utils.AggregateFunction.COUNT;

/**
 * @author mnesimiyilmaz
 */
public class GroupByProcessor {

    private final GroupByInput groupByInput;

    private final Map<String, FieldKey>        groupByFieldCache;
    private final List<SelectColumnDefinition> nonAggColumnsCache;
    private final List<SelectColumnDefinition> aggColumnsCache;

    public GroupByProcessor(GroupByInput groupByInput) {
        this.groupByInput = groupByInput;
        this.groupByFieldCache = new HashMap<>();
        this.groupByInput.getGroupByColumns()
                .forEach(x -> groupByFieldCache.put(x.getColumnName(), FieldKey.of(x.getColumnName())));
        this.nonAggColumnsCache = groupByInput.getSelectedColumns().stream()
                .filter(x -> x.getColumnDefinition().getAggregateFunction() == null).collect(Collectors.toList());
        this.aggColumnsCache = groupByInput.getSelectedColumns()
                .stream().filter(x -> x.getColumnDefinition().getAggregateFunction() != null).collect(Collectors.toList());
    }

    public List<Map<FieldKey, Object>> process() {
        Map<GroupRowData, List<Map<FieldKey, Object>>> groupedRows = groupRows();
        List<Map<FieldKey, Object>> groupByResult = applyAggregations(groupedRows);
        if (groupByInput.getHavingClause() != null) {
            groupByResult.removeIf(x -> !groupByInput.getHavingClause().test(x));
        }
        return groupByResult;
    }

    private Map<GroupRowData, List<Map<FieldKey, Object>>> groupRows() {
        Map<GroupRowData, List<Map<FieldKey, Object>>> groupedRows = new HashMap<>();
        for (Map<FieldKey, Object> row : this.groupByInput.getFlattenedInputJsonNode()) {
            Map<String, Object> groupRowData = new HashMap<>();
            for (JsonColumnWithNonAggFunctionDefinion groupByColumn : this.groupByInput.getGroupByColumns()) {
                Object val = row.get(this.groupByFieldCache.get(groupByColumn.getColumnName()));
                if (groupByColumn.getValueDecorator().isPresent()) {
                    val = groupByColumn.getValueDecorator().get().apply(val);
                }
                groupRowData.put(groupByColumn.getColumnName(), val);
            }
            groupedRows.computeIfAbsent(new GroupRowData(groupRowData), x -> new ArrayList<>()).add(row);
        }
        return groupedRows;
    }

    private List<Map<FieldKey, Object>> applyAggregations(Map<GroupRowData, List<Map<FieldKey, Object>>> groupedRows) {
        List<Map<FieldKey, Object>> aggResult = new ArrayList<>();
        for (Map.Entry<GroupRowData, List<Map<FieldKey, Object>>> groupRow : groupedRows.entrySet()) {
            aggResult.add(applyAggregationForAGroup(groupRow.getKey(), groupRow.getValue()));
        }
        return aggResult;
    }

    private Map<FieldKey, Object> applyAggregationForAGroup(GroupRowData groupRowData, List<Map<FieldKey, Object>> rows) {
        Map<FieldKey, Object> result = new HashMap<>();
        nonAggColumnsCache.forEach(x -> {
            String ocn = x.getColumnDefinition().getColumnDefinition().getColumnName();
            result.put(getAliasOrElseOriginalColumnName(x), groupRowData.getColumnNameValuePairs().get(ocn));
        });
        for (SelectColumnDefinition aggColumn : aggColumnsCache) {
            if (aggColumn.getColumnDefinition().isAsterisk() && COUNT.equals(aggColumn.getColumnDefinition().getAggregateFunction())) {
                result.put(getAliasOrElseOriginalColumnName(aggColumn), (double) rows.size());
                continue;
            }
            String columnName = aggColumn.getColumnDefinition().getColumnDefinition().getColumnName();
            Optional<Function<Object, Object>> decorator = aggColumn.getColumnDefinition().getColumnDefinition().getValueDecorator();
            AggregateFunction aggFunc = aggColumn.getColumnDefinition().getAggregateFunction();
            List<Object> vals = rows.stream()
                    .flatMap(x -> x.entrySet().stream().filter(y -> y.getKey().getFamily().equals(columnName)))
                    .map(z -> decorator.isPresent() ? decorator.get().apply(z.getValue()) : z.getValue())
                    .filter(Objects::nonNull).collect(Collectors.toList());
            result.put(getAliasOrElseOriginalColumnName(aggColumn), AggregationUtils.getAggregationFunction(aggFunc).apply(vals));
        }
        return result;
    }

    private FieldKey getAliasOrElseOriginalColumnName(SelectColumnDefinition scd) {
        if (scd.getColumnDefinition().isAsterisk()) {
            if (scd.getAlias() != null) {
                return FieldKey.of(scd.getAlias());
            }
            return FieldKey.of(scd.getCtx().getText());
        }
        String ocn = scd.getColumnDefinition().getColumnDefinition().getColumnName();
        String alias = scd.getAlias();
        return FieldKey.of(alias != null ? alias : ocn);
    }

}
