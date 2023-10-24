package io.github.mnesimiyilmaz.sql4json.definitions;

import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import lombok.Getter;

/**
 * @author mnesimiyilmaz
 */
@Getter
public class SelectColumnDefinition {

    private final SQL4JsonParser.SelectColumnContext    ctx;
    private final JsonColumnWithAggFunctionDefinion columnDefinition;
    private final String                            alias;
    private final boolean                           isAsterisk;

    private SelectColumnDefinition() {
        this.ctx = null;
        this.columnDefinition = null;
        this.alias = null;
        this.isAsterisk = true;
    }

    public SelectColumnDefinition(SQL4JsonParser.SelectColumnContext ctx) {
        this.ctx = ctx;
        this.columnDefinition = new JsonColumnWithAggFunctionDefinion(ctx.jsonColumnWithAggFunction());
        if (ctx.AS() != null) {
            this.alias = ctx.jsonColumn().getText();
        } else {
            this.alias = null;
        }
        this.isAsterisk = false;
    }

    public static SelectColumnDefinition ofAsterisk() {
        return new SelectColumnDefinition();
    }

}
