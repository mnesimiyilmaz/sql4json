grammar SQL4Json;

sql4json
    : SELECT selectedColumns FROM rootNode (WHERE whereConditions)? (GROUP BY groupByColumns)? (HAVING havingConditions)? (ORDER BY orderByColumns)? (SEMI_COLON)? EOF
    ;

rootNode
    : ROOT (DOT IDENTIFIER (DOT IDENTIFIER)*)?
    ;

jsonColumn
    : IDENTIFIER (DOT IDENTIFIER)*
    ;

params
    : value (COMMA value)*
    ;

jsonColumnWithNonAggFunction
    : jsonColumn
    | NON_AGG_FUNCTION LPAREN jsonColumn (COMMA params)? RPAREN
    ;

jsonColumnWithAggFunction
    : jsonColumnWithNonAggFunction
    | AGG_FUNCTION LPAREN (ASTERISK | jsonColumnWithNonAggFunction) RPAREN
    ;

selectColumn
    : jsonColumnWithAggFunction (AS jsonColumn)?
    ;

selectedColumns
    : ASTERISK
    | selectColumn (COMMA selectColumn)*
    ;

value
    : STRING | NUMBER | BOOLEAN | NULL | VALUE_FUNCTION
    ;

valueWithNonAggFunction
    : value
    | NON_AGG_FUNCTION LPAREN value (COMMA params)? RPAREN
    ;

comparison
    : jsonColumnWithNonAggFunction COMPARISON_OPERATOR valueWithNonAggFunction
    ;

like
    : jsonColumnWithNonAggFunction LIKE STRING
    ;

isNull
    : jsonColumnWithNonAggFunction IS NULL
    ;

isNotNull
    : jsonColumnWithNonAggFunction IS NOT NULL
    ;

condition
    : comparison
    | like
    | isNull
    | isNotNull
    ;

nestedCondition
    : LPAREN (condition | nestedCondition) (LOGICAL_OPERATOR (condition | nestedCondition))* RPAREN
    ;

conditions
    : (condition | nestedCondition) (LOGICAL_OPERATOR (condition | nestedCondition))*
    ;

whereConditions
    : conditions
    ;

havingConditions
    : conditions
    ;

groupByColumn
    : jsonColumnWithNonAggFunction
    ;

groupByColumns
    : groupByColumn (COMMA groupByColumn)*
    ;

orderByColumn
    : jsonColumnWithNonAggFunction (ORDER_DIRECTION)?
    ;

orderByColumns
    : orderByColumn (COMMA orderByColumn)*
    ;

SELECT: S E L E C T ;
FROM: F R O M ;
WHERE: W H E R E ;
AS: A S;
GROUP: G R O U P;
BY: B Y;
ORDER: O R D E R;
HAVING: H A V I N G;
LIKE: L I K E;
IS: I S;
NOT: N O T;
ROOT: DOLLAR_SIGN R;
ORDER_DIRECTION: ASC | DESC;
NON_AGG_FUNCTION: STRING_FUNCTION | UTILITY_FUNCTION;
STRING_FUNCTION: LOWER | UPPER;
UTILITY_FUNCTION: COALESCE | TO_DATE;
AGG_FUNCTION: AVG | SUM | COUNT | MIN | MAX;
VALUE_FUNCTION: NOW;
SEMI_COLON: ';';
COMMA: ',';
DOT: '.';
ASTERISK: '*';
LPAREN : '(' ;
RPAREN : ')' ;
STRING : '\'' ~[']* '\'';
NUMBER: [0-9.]+;
BOOLEAN: TRUE | FALSE;
NULL: N U L L;
COMPARISON_OPERATOR: GTE | LTE | NOT_EQUAL | GT | LT | EQUAL;
LOGICAL_OPERATOR: AND | OR;
IDENTIFIER: [A-Za-z0-9_-]+;
ESC: [ \t\r\n]+ -> skip;

fragment DOLLAR_SIGN: '$';
fragment COALESCE: C O A L E S C E;
fragment NOW: N O W LPAREN RPAREN;
fragment ASC: A S C;
fragment DESC: D E S C;
fragment AVG: A V G;
fragment SUM: S U M;
fragment COUNT: C O U N T;
fragment MIN: M I N;
fragment MAX: M A X;
fragment LOWER: L O W E R;
fragment UPPER: U P P E R;
fragment TO_DATE: T O UNDER_SCORE D A T E;
fragment TRUE: T R U E;
fragment FALSE: F A L S E;
fragment GTE: '>=';
fragment LTE: '<=';
fragment NOT_EQUAL: '!=';
fragment GT: '>';
fragment LT: '<';
fragment EQUAL: '=';
fragment AND: A N D;
fragment OR: O R;
fragment UNDER_SCORE: '_';
fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');