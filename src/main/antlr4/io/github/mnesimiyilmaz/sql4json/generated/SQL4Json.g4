grammar SQL4Json;

options {
    caseInsensitive = true;
}

sql4json
    : SELECT DISTINCT? selectedColumns FROM rootNode joinClause*
      (WHERE whereConditions)?
      (GROUP BY groupByColumns)?
      (HAVING havingConditions)?
      (ORDER BY orderByColumns)?
      (LIMIT limitValue (OFFSET limitValue)?)?
      (SEMI_COLON)? EOF?
    ;

rootNode
    : ROOT (DOT jsonColumn)*
    | LPAREN sql4json RPAREN
    | identifierOrKeyword IDENTIFIER?
    ;

jsonColumn
    : identifierOrKeyword (DOT identifierOrKeyword)*
    ;

identifierOrKeyword
    : IDENTIFIER | INNER | LEFT | RIGHT | JOIN | ON
    | OVER | PARTITION | ROW_NUMBER | RANK | DENSE_RANK | NTILE | LAG | LEAD
    | CASE | WHEN | THEN | ELSE | END
    ;

joinClause
    : joinType? JOIN identifierOrKeyword IDENTIFIER? ON joinCondition
    ;

joinType
    : INNER
    | LEFT
    | RIGHT
    ;

joinCondition
    : joinEquality (AND joinEquality)*
    ;

joinEquality
    : jsonColumn COMPARISON_OPERATOR jsonColumn
    ;

// Column expression: a plain column OR a scalar function applied to a column
// FunctionCallExpr: LOWER(name), UPPER(name), COALESCE(name, 'default'), TO_DATE(dateCol, 'fmt')
// CastExpr: CAST(col AS STRING), CAST(col AS NUMBER), etc.
// SimpleColumnExpr: name, address.city, etc.
columnExpr
    : caseExpr                                                    # CaseExprColumn
    | windowFunctionCall                                          # WindowFunctionExpr
    | AGG_FUNCTION LPAREN (ASTERISK | columnExpr) RPAREN         # AggFunctionColumnExpr
    | functionCall                                                # FunctionCallExpr
    | castExpr                                                    # CastExprColumn
    | jsonColumn                                                  # SimpleColumnExpr
    | value                                                       # LiteralColumnExpr
    ;

caseExpr
    : CASE (WHEN conditions THEN columnExpr)+ (ELSE columnExpr)? END              // searched (MUST be first)
    | CASE columnExpr (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END   // simple
    ;

windowFunctionCall
    : windowFunctionName LPAREN windowFunctionArgs? RPAREN OVER LPAREN windowSpec RPAREN
    ;

windowFunctionName
    : ROW_NUMBER | RANK | DENSE_RANK | NTILE
    | LAG | LEAD
    | AGG_FUNCTION
    ;

windowFunctionArgs
    : columnExpr (COMMA columnExpr)*
    | ASTERISK
    ;

windowSpec
    : (PARTITION BY columnExpr (COMMA columnExpr)*)?
      (ORDER BY orderByColumn (COMMA orderByColumn)*)?
    ;

castExpr
    : CAST LPAREN columnExpr AS castType RPAREN
    ;

castType
    : STRING_TYPE | NUMBER_TYPE | INTEGER_TYPE | DECIMAL_TYPE
    | BOOLEAN_TYPE | DATE_TYPE | DATETIME_TYPE
    ;

// Generic scalar function call: IDENTIFIER(arg, arg, ...) — validated at runtime by FunctionRegistry
functionCall
    : identifierOrKeyword LPAREN functionArgs? RPAREN    # ScalarFunctionCall
    ;

functionArg
    : jsonColumnWithAggFunction    # ExprFunctionArg
    | value                        # ValueFunctionArg
    ;

functionArgs
    : functionArg (COMMA functionArg)*
    ;

// Column expression that may carry an aggregate function (used in SELECT only)
jsonColumnWithAggFunction
    : windowFunctionCall                                         # WindowFunctionAggExpr
    | AGG_FUNCTION LPAREN (ASTERISK | columnExpr) RPAREN        # AggFunctionExpr
    | columnExpr                                                 # NonAggExpr
    ;

selectColumn
    : jsonColumnWithAggFunction (AS jsonColumn)?
    ;

selectedColumns
    : ASTERISK
    | selectColumn (COMMA selectColumn)*
    ;

value
    : STRING | NUMBER | BOOLEAN | NULL | VALUE_FUNCTION | parameter
    ;

limitValue
    : NUMBER
    | parameter
    ;

parameter
    : POSITIONAL_PARAM
    | NAMED_PARAM
    ;

// Right-hand side of a comparison: a plain value or a function applied to a value
// e.g., WHERE date = TO_DATE('2024-01-01', 'yyyy-MM-dd')
rhsValue
    : value         # RhsPlainValue
    | functionCall  # RhsFunctionCall
    | castExpr      # RhsCastExpr
    | jsonColumn    # RhsColumnRef
    ;

comparison
    : columnExpr COMPARISON_OPERATOR rhsValue
    ;

like
    : columnExpr LIKE rhsValue
    ;

notLike
    : columnExpr NOT LIKE rhsValue
    ;

isNull
    : columnExpr IS NULL
    ;

isNotNull
    : columnExpr IS NOT NULL
    ;

in
    : columnExpr IN LPAREN rhsValue (COMMA rhsValue)* RPAREN
    ;

notIn
    : columnExpr NOT IN LPAREN rhsValue (COMMA rhsValue)* RPAREN
    ;

between
    : columnExpr BETWEEN rhsValue AND rhsValue
    ;

notBetween
    : columnExpr NOT BETWEEN rhsValue AND rhsValue
    ;

condition
    : comparison    # ComparisonCondition
    | like          # LikeCondition
    | notLike       # NotLikeCondition
    | isNull        # IsNullCondition
    | isNotNull     # IsNotNullCondition
    | in            # InCondition
    | notIn         # NotInCondition
    | between       # BetweenCondition
    | notBetween    # NotBetweenCondition
    ;

// ANTLR implicit left-recursive precedence: first alternative = highest precedence
// AND (highest) > OR (lower) — standard SQL semantics
conditions
    : conditions AND conditions         # AndConditions
    | conditions OR conditions          # OrConditions
    | LPAREN conditions RPAREN          # ParenConditions
    | condition                         # SingleCondition
    ;

whereConditions
    : conditions
    ;

havingConditions
    : conditions
    ;

groupByColumn
    : columnExpr
    ;

groupByColumns
    : groupByColumn (COMMA groupByColumn)*
    ;

orderByColumn
    : columnExpr (ORDER_DIRECTION)?
    ;

orderByColumns
    : orderByColumn (COMMA orderByColumn)*
    ;

// ── Lexer rules ──────────────────────────────────────────────────────────────

SELECT          : 'SELECT';
FROM            : 'FROM';
WHERE           : 'WHERE';
AS              : 'AS';
GROUP           : 'GROUP';
BY              : 'BY';
ORDER           : 'ORDER';
HAVING          : 'HAVING';
LIKE            : 'LIKE';
IS              : 'IS';
NOT             : 'NOT';
AND             : 'AND';
OR              : 'OR';
NULL            : 'NULL';
ROOT            : '$R';
ORDER_DIRECTION : 'ASC' | 'DESC';
AGG_FUNCTION    : 'AVG' | 'SUM' | 'COUNT' | 'MIN' | 'MAX';
VALUE_FUNCTION  : 'NOW' LPAREN RPAREN;
BOOLEAN         : 'TRUE' | 'FALSE';
DISTINCT        : 'DISTINCT';
LIMIT           : 'LIMIT';
OFFSET          : 'OFFSET';
IN              : 'IN';
BETWEEN         : 'BETWEEN';
CAST            : 'CAST';
STRING_TYPE     : 'STRING';
NUMBER_TYPE     : 'NUMBER';
INTEGER_TYPE    : 'INTEGER';
DECIMAL_TYPE    : 'DECIMAL';
BOOLEAN_TYPE    : 'BOOLEAN';
DATE_TYPE       : 'DATE';
DATETIME_TYPE   : 'DATETIME';

INNER : 'INNER' ;
JOIN  : 'JOIN' ;
LEFT  : 'LEFT' ;
RIGHT : 'RIGHT' ;
ON    : 'ON' ;

OVER       : 'OVER' ;
PARTITION  : 'PARTITION' ;
ROW_NUMBER : 'ROW_NUMBER' ;
RANK       : 'RANK' ;
DENSE_RANK : 'DENSE_RANK' ;
NTILE      : 'NTILE' ;
LAG        : 'LAG' ;
LEAD       : 'LEAD' ;

CASE      : 'CASE' ;
WHEN      : 'WHEN' ;
THEN      : 'THEN' ;
ELSE      : 'ELSE' ;
END       : 'END' ;

COMPARISON_OPERATOR : '>=' | '<=' | '!=' | '>' | '<' | '=';
SEMI_COLON  : ';';
COMMA       : ',';
DOT         : '.';
ASTERISK    : '*';
LPAREN      : '(';
RPAREN      : ')';
NUMBER           : '-'? DIGIT+ ('.' DIGIT+)?;
STRING           : '\'' ( ~('\'') | '\'\'' )* '\'';
POSITIONAL_PARAM : '?';
NAMED_PARAM      : ':' (LETTER | '_') (LETTER | DIGIT | '_')* ;
IDENTIFIER       : (LETTER | '_') (LETTER | DIGIT | '_' | '-')*;
ESC         : [ \t\r\n]+ -> skip;

fragment DIGIT  : [0-9];
fragment LETTER : [a-z];  // caseInsensitive=true expands to [a-zA-Z] at runtime
