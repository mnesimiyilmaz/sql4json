package io.github.mnesimiyilmaz.sql4json;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonLexer;
import io.github.mnesimiyilmaz.sql4json.generated.SQL4JsonParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 * @author mnesimiyilmaz
 */
public class SQL4JsonProcessor {

    private final String[] queries;

    private SQL4JsonInput sql4JsonInput;
    private int           currentQuery;
    private JsonNode  result;

    public SQL4JsonProcessor(SQL4JsonInput sql4JsonInput) {
        this.queries = sql4JsonInput.getSql().split(">>>");
        this.currentQuery = queries.length - 1;
        this.sql4JsonInput = new SQL4JsonInput(queries[currentQuery], sql4JsonInput.getRootNode());
    }

    public JsonNode getResult() {
        if (result != null) return result;

        do {
            SQL4JsonLexer lexer = new SQL4JsonLexer(CharStreams.fromString(sql4JsonInput.getSql()));
            SQL4JsonParser parser = new SQL4JsonParser(new CommonTokenStream(lexer));
            SQL4JsonListenerImpl listener = new SQL4JsonListenerImpl(sql4JsonInput.getRootNode());
            new ParseTreeWalker().walk(listener, parser.sql4json());
            result = listener.getResult();
            if (--currentQuery >= 0) {
                sql4JsonInput = new SQL4JsonInput(queries[currentQuery], result);
            }
        } while (currentQuery >= 0);

        return result;
    }

}
