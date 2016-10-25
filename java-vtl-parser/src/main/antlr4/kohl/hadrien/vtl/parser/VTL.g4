/*-
 * #%L
 * Java VTL
 * %%
 * Copyright (C) 2016 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
grammar VTL;

import Atoms, Clauses, Conditional, Relational;

start : statement+ EOF;

/* Assignment */
statement : variableRef ':=' datasetExpression;

exprMember : datasetExpression ('#' componentID)? ;

/* Expressions */
datasetExpression : <assoc=right>datasetExpression clauseExpression #withClause
           | relationalExpression                                   #withRelational
           | getExpression                                          #withGet
           | putExpression                                          #withPut
           | exprAtom                                               #withAtom
           ;

componentID : IDENTIFIER;

getExpression : 'get' '(' datasetId ')';
putExpression : 'put(todo)';

datasetId : STRING_CONSTANT ;

WS : [ \r\t\u000C] -> skip ;
