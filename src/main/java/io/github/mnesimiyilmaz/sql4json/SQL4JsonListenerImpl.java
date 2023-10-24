package io.github.mnesimiyilmaz.sql4json;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.mnesimiyilmaz.sql4json.condition.ConditionProcessor;
import io.github.mnesimiyilmaz.sql4json.condition.CriteriaNode;
import io.github.mnesimiyilmaz.sql4json.definitions.JsonColumnWithNonAggFunctionDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.OrderByColumnDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.SelectColumnDefinition;
import io.github.mnesimiyilmaz.sql4json.grouping.GroupByInput;
import io.github.mnesimiyilmaz.sql4json.grouping.GroupByProcessor;
import io.github.mnesimiyilmaz.sql4json.sorting.SortProcessor;
import io.github.mnesimiyilmaz.sql4json.utils.FieldKey;
import io.github.mnesimiyilmaz.sql4json.utils.JsonUtils;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonBaseListener;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author mnesimiyilmaz
 */
class SQL4JsonListenerImpl extends SQL4JsonBaseListener {

    private final List<SelectColumnDefinition> selectedColumns;

    private JsonNode                                   dataNode;
    private List<Map<FieldKey, Object>>                flattenedData;
    private CriteriaNode                               whereClause;
    private List<JsonColumnWithNonAggFunctionDefinion> groupByColumns;
    private CriteriaNode                               havingClause;
    private List<OrderByColumnDefinion>                orderByColumns;

    public SQL4JsonListenerImpl(JsonNode jsonNode) {
        this.dataNode = jsonNode;
        this.selectedColumns = new ArrayList<>();
    }

    @Override
    public void enterRootNode(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.RootNodeContext ctx) {
        String rootPath = ctx.getText();
        if (rootPath.equalsIgnoreCase("$r")) {
            rootPath = "";
        } else {
            rootPath = rootPath.substring(3);
        }
        this.dataNode = JsonUtils.peelJsonNode(dataNode, rootPath);
        this.flattenedData = JsonUtils.convertJsonToFlattenedListOfKeyValue(dataNode);
    }

    @Override
    public void enterSelectColumn(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.SelectColumnContext ctx) {
        selectedColumns.add(new SelectColumnDefinition(ctx));
    }

    @Override
    public void enterSelectedColumns(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.SelectedColumnsContext ctx) {
        if (ctx.ASTERISK() != null) {
            selectedColumns.add(SelectColumnDefinition.ofAsterisk());
        }
    }

    @Override
    public void enterWhereConditions(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.WhereConditionsContext ctx) {
        this.whereClause = new ConditionProcessor(ctx.conditions()).process();
    }

    @Override
    public void enterHavingConditions(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.HavingConditionsContext ctx) {
        this.havingClause = new ConditionProcessor(ctx.conditions()).process();
    }

    @Override
    public void enterGroupByColumn(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.GroupByColumnContext ctx) {
        if (this.groupByColumns == null) {
            this.groupByColumns = new ArrayList<>();
        }
        this.groupByColumns.add(new JsonColumnWithNonAggFunctionDefinion(ctx.jsonColumnWithNonAggFunction()));
    }

    @Override
    public void enterOrderByColumn(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.OrderByColumnContext ctx) {
        if (this.orderByColumns == null) {
            this.orderByColumns = new ArrayList<>();
        }
        this.orderByColumns.add(new OrderByColumnDefinion(ctx));
    }

    public JsonNode getResult() {
        if (whereClause != null) {
            this.flattenedData.removeIf(x -> !whereClause.test(x));
        }
        if (groupByColumns != null) {
            this.flattenedData = new GroupByProcessor(new GroupByInput(
                    flattenedData, selectedColumns, groupByColumns, havingClause)).process();
        }
        if (orderByColumns != null) {
            flattenedData.sort(new SortProcessor(flattenedData, orderByColumns).buildComparator());
        }
        if (groupByColumns == null && !selectedColumns.get(0).isAsterisk()) {
            flattenedData = flattenedData.stream().map(this::getSelectedRowData).collect(Collectors.toList());
        }
        return JsonUtils.convertStructuredMapToJsonNode(
                flattenedData.stream().map(JsonUtils::convertFlatMapToStructuredMap).collect(Collectors.toList()));
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
