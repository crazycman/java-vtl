package no.ssb.vtl.parser;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
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
 * =========================LICENSE_END==================================
 */

/*-
 * #%L
 * java-vtl-parser
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

import org.junit.Test;

public class ClausesParserTest extends GrammarTest {

    @Test
    public void testRenameWithRole() throws Exception {
        parse("[rename varId as varId role IDENTIFIER]", "clauseExpression");
        parse("[rename varId as varId role MEASURE]", "clauseExpression");
        parse("[rename varId as varId role ATTRIBUTE]", "clauseExpression");
    }

    @Test
    public void testMultipleRenames() throws Exception {
        parse("[rename varId as varId, varId as varId, varId as varId]", "clauseExpression");
    }

    @Test
    public void testMultipleRenamesWithRoles() throws Exception {
        parse("[rename varId as varId role IDENTIFIER," +
                        "    varId as varId role MEASURE," +
                        "    varId as varId role ATTRIBUTE]",
                "clauseExpression");
    }

}
