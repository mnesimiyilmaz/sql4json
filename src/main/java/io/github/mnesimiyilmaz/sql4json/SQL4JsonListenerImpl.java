package io.github.mnesimiyilmaz.sql4json;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.mnesimiyilmaz.sql4json.condition.ConditionProcessor;
import io.github.mnesimiyilmaz.sql4json.definitions.JsonColumnWithNonAggFunctionDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.OrderByColumnDefinion;
import io.github.mnesimiyilmaz.sql4json.definitions.SelectColumnDefinition;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import io.github.mnesimiyilmaz.sql4json.processor.SQLBuilder;
import io.github.mnesimiyilmaz.sql4json.processor.SQLProcessor;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.*;

/**
 * @author mnesimiyilmaz
 */
class SQL4JsonListenerImpl extends SQL4JsonBaseListener {

    private final List<SelectColumnDefinition> selectedColumns;
    private final JsonNode dataNode;
    private final SQLProcessor sqlProcessor;
    private String rootPath;

    public SQL4JsonListenerImpl(JsonNode jsonNode) {
        this.dataNode = jsonNode;
        this.sqlProcessor = new SQLProcessor();
        this.selectedColumns = new ArrayList<>();
    }

    @Override
    public void enterRootNode(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.RootNodeContext ctx) {
        String path = ctx.getText();
        if(path.startsWith("$r")){
            this.rootPath = path.equalsIgnoreCase("$r") ? "" : path.substring(3);
        }
        getBuilder(ctx).setSelectedColumns(new ArrayList<>(selectedColumns));
        selectedColumns.clear();
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
        getBuilder(ctx).setWhereClause(new ConditionProcessor(ctx.conditions()).process());
    }

    @Override
    public void enterHavingConditions(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.HavingConditionsContext ctx) {
        getBuilder(ctx).setHavingClause(new ConditionProcessor(ctx.conditions()).process());
    }

    @Override
    public void enterGroupByColumn(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.GroupByColumnContext ctx) {
        getBuilder(ctx).addGroupByColumn(new JsonColumnWithNonAggFunctionDefinion(ctx.jsonColumnWithNonAggFunction()));
    }

    @Override
    public void enterOrderByColumn(io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser.OrderByColumnContext ctx) {
        getBuilder(ctx).addOrderByColumn(new OrderByColumnDefinion(ctx));
    }

    public JsonNode getResult(){
        return sqlProcessor.process(dataNode, rootPath);
    }

    private SQLBuilder getBuilder(ParseTree tree){
        return sqlProcessor.getBuilder(sql4JsonContextNameOf(tree));
    }

    private static String sql4JsonContextNameOf(ParseTree tree){
        ParseTree parent = tree.getParent();

        while (!(parent instanceof SQL4JsonParser.Sql4jsonContext)){
            parent = parent.getParent();
        }
        return parent.toString();
    }

}
