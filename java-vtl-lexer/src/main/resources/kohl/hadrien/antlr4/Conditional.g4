grammar Conditional;

expression : booleanExpression EOF ;
//
booleanExpression
    : booleanExpression AND booleanExpression
    | booleanExpression ( OR booleanExpression | XOR booleanExpression )
    | booleanEquallity
    | BOOLEAN_CONSTANT
    ;

booleanEquallity
    : booleanEquallity ( ( EQ | NE | LE | GE ) booleanEquallity )
    | datasetExpression
    // typed constant?
    ;

datasetExpression
    : 'dsTest'
    ;

EQ : '='  ;
NE : '<>' ;
LE : '<=' ;
GE : '>=' ;

AND : 'and' ;
OR  : 'or' ;
XOR : 'xor' ;

BOOLEAN_CONSTANT : 'true' | 'false' ;

WS : [ \r\t\u000C] -> skip ;