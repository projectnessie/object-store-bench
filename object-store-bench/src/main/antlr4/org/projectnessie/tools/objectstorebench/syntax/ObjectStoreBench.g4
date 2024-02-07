/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
grammar ObjectStoreBench;

options { caseInsensitive = true; }

@header {
// PACKAGE_PLACEHOLDER
}

script
    :
      ( statement ';' )* (statement ';'?)?
      EOF
    ;

statement
    : putStatement
    | reuseStatement
    | getStatement
    | deleteStatement
    ;

putStatement
    : 'PUT'
        parms=objectParams
        namingStrategyExpr?
        sizeExpr?
    ;

reuseStatement
    : 'REUSE'
        numObjectsExpr
        contextExpr?
        namingStrategyExpr
    ;

getStatement
    : 'GET'
        parms=objectParams
    ;

deleteStatement
    : 'DELETE'
        parms=objectParams
    ;

objectParams
    : numObjectsExpr?
        rateExpr?
        concurrentExpr?
        runtimeExpr?
        contextExpr?
    ;

namingStrategyExpr
    : 'USING' 'NAMING' 'STRATEGY'
        ( nsConstant=namingStrategyConstant | nsRandmom=namingStrategyRandom )
        ( 'WITH' 'SEED' seed=nameValue )?
    ;

numObjectsExpr
    : numObjects=INT_LITERAL ('OBJECTS' | 'OBJECT')
    ;

rateExpr
    : 'AT' 'RATE' rate=INT_LITERAL ('PER' rateUnit=TIME_UNIT)?
        ( 'WARMUP' warmup=durationExpr )?
    ;

runtimeExpr
    : 'RUNTIME' duration=durationExpr
    ;

durationExpr
    : durationValue=INT_LITERAL (durationUnit=TIME_UNIT)?
    ;

concurrentExpr
    : 'MAX' maxConcurrent=INT_LITERAL 'CONCURRENT'
    ;

contextExpr
    : 'IN' 'CONTEXT' context=nameValue
    ;

sizeExpr
    : 'OF' 'SIZE' sizeValue=INT_LITERAL (sizeUnit=SIZE_UNIT)?
    ;

nameValue
    : '"' nameLiteralValue=NAME_LITERAL '"'
    ;

namingStrategyRandom
    : 'RANDOM' 'PREFIX'
    ;

namingStrategyConstant
    : 'CONSTANT' 'PREFIX'
    ;

SIZE_UNIT
    : 'BYTE' | 'BYTES' | 'B'
    | 'KILOBYTES' | 'KILOBYTE' | 'K' | 'KB'
    | 'MEGABYTES' | 'MEGABYTE' | 'M' | 'MB'
    | 'GIGABYTES' | 'GIGABYTE' | 'G' | 'GB'
    ;

TIME_UNIT
    : 'MILLISECOND' | 'SECOND' | 'MINUTE' | 'HOUR'
    | 'MILLISECONDS' | 'SECONDS' | 'MINUTES' | 'HOURS'
    | 'MS' | 'SEC' | 'MIN'
    ;

INT_LITERAL
    : DIGIT+
    ;

NAME_LITERAL options { caseInsensitive=false; }
    : (LETTER | DIGIT)+
    ;

FLOAT_LITERAL
    : DIGIT+ EXPONENT?
    | DECIMAL_DIGITS EXPONENT?
    ;

fragment LETTER
    : [A-Z]
    ;

fragment DIGIT
    : [0-9]
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

SIMPLE_COMMENT
    : ('--' | '//' | '#') ('\\\n' | ~[\r\n])* '\r'? '\n'? -> skip
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> skip
    ;

WS
    : [ \r\n\t\p{White_Space}]+ -> skip
    ;

// Catch-all for anything we can't recognize.
UNRECOGNIZED
    : .
    ;
