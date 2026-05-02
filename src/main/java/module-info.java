/** SQL4Json JPMS module — query JSON data using SQL. */
module io.github.mnesimiyilmaz.sql4json {
    requires org.antlr.antlr4.runtime;

    exports io.github.mnesimiyilmaz.sql4json;
    exports io.github.mnesimiyilmaz.sql4json.exception;
    exports io.github.mnesimiyilmaz.sql4json.grammar;
    exports io.github.mnesimiyilmaz.sql4json.settings;
    exports io.github.mnesimiyilmaz.sql4json.types;
}
