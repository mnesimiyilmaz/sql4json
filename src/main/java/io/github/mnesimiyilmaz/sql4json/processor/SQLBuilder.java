package io.github.mnesimiyilmaz.sql4json.processor;

import io.github.mnesimiyilmaz.sql4json.condition.CriteriaNode;
import io.github.mnesimiyilmaz.sql4json.definitions.JsonColumnWithNonAggFunctionDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.OrderByColumnDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.SelectColumnDefinition;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class SQLBuilder {

    @Setter
    private List<SelectColumnDefinition> selectedColumns;
    @Setter
    private CriteriaNode whereClause;
    private List<JsonColumnWithNonAggFunctionDefinion> groupByColumns;
    @Setter
    private CriteriaNode havingClause;
    private List<OrderByColumnDefinion> orderByColumns;

    public void addGroupByColumn(JsonColumnWithNonAggFunctionDefinion groupByColumn){
        if(this.groupByColumns == null)
            this.groupByColumns = new ArrayList<>();

        this.groupByColumns.add(groupByColumn);
    }

    public void addOrderByColumn(OrderByColumnDefinion orderByColumn){
        if(this.orderByColumns == null)
            this.orderByColumns = new ArrayList<>();

        this.orderByColumns.add(orderByColumn);
    }
}
