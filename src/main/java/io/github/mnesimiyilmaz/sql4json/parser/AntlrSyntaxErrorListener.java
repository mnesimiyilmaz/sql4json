package io.github.mnesimiyilmaz.sql4json.parser;

import io.github.mnesimiyilmaz.sql4json.exception.SQL4JsonParseException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * ANTLR error listener that converts syntax errors into {@link SQL4JsonParseException}.
 */
public class AntlrSyntaxErrorListener extends BaseErrorListener {

    /**
     * Creates a new ANTLR syntax error listener.
     */
    public AntlrSyntaxErrorListener() {
        // No state to initialize; behaviour is provided by the overridden syntaxError hook.
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                            Object offendingSymbol,
                            int line,
                            int charPositionInLine,
                            String msg,
                            RecognitionException e) {
        throw new SQL4JsonParseException(msg + " at line " + line + ":" + charPositionInLine, line, charPositionInLine);
    }

}
