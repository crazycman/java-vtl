package no.ssb.vtl.script.operations;

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

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.operations.join.InnerJoinOperation;
import org.assertj.core.api.SoftAssertions;

import java.util.Arrays;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.*;
import static org.mockito.Mockito.*;

/**
 * Created by hadrien on 21/11/2016.
 */
public class SumOperationTest {

    ObjectMapper mapper = new ObjectMapper();

    /**
     * Both Datasets must have at least one Identifier Component
     * in common (with the same name and data type).
     * <p>
     * VTL 1.1 line 2499.
     */
    //@Test
    public void testIdenfitierNotASubset() throws Exception {

        Dataset left = mock(Dataset.class);
        Dataset right = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1", Role.MEASURE, Long.class
            ));
            Throwable expectedThrowable = null;

            // Different name
            when(right.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1DIFFERENTNAME", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1", Role.MEASURE, Long.class
            ));
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, left.getDataStructure(), tuple -> null, right.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common id name")
                    .isNotNull()
                    .hasMessageContaining("ID1DIFFERENTNAME");

            // any order
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, right.getDataStructure(), tuple -> null, left.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common id name (reversed)")
                    .isNotNull()
                    .hasMessageContaining("ID1DIFFERENTNAME");

            // Different type
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1", Role.MEASURE, Long.class
            ));
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, left.getDataStructure(), tuple -> null, right.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common id type")
                    .isNotNull();

            // any order.
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, right.getDataStructure(), tuple -> null, left.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common id type (reversed)")
                    .isNotNull();

        } finally {
            softly.assertAll();
        }
    }

    /**
     * If both ds_1 and ds_2 are Datasets then either they have
     * one or more measures in common, or at least one of them
     * has only a measure.
     * <p>
     * VTL 1.1 line 2501.
     */
    //@Test
    public void testNoCommonMeasure() throws Exception {

        Dataset left = mock(Dataset.class);
        Dataset right = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1", Role.MEASURE, Long.class
            ));
            Throwable expectedThrowable = null;

            // Different measure
            when(right.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "ID1", Role.IDENTIFIER, String.class,
                    "ID2", Role.IDENTIFIER, String.class,
                    "ME1NOTSAMEMEASURE", Role.MEASURE, Long.class
            ));
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, left.getDataStructure(), tuple -> null, right.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common measure name")
                    .isNotNull()
                    .hasMessageContaining("ME1NOTSAMEMEASURE");

            // any order
            expectedThrowable = null;
            try {
                new SumOperation(tuple -> null, right.getDataStructure(), tuple -> null, left.getDataStructure());
            } catch (Throwable t) {
                expectedThrowable = t;
            }
            softly.assertThat(expectedThrowable)
                    .as("SumOperation exception when no common measure name (reversed)")
                    .isNotNull();

        } finally {
            softly.assertAll();
        }
    }

    /**
     * Test the example 1
     * <p>
     * VTL 1.1 line 2522-2536.
     *
     * @throws Exception
     */
    //@Test()
    public void testSumEx1() throws Exception {

        Dataset left = mock(Dataset.class);
        Dataset right = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "TIME", Role.IDENTIFIER, String.class,
                    "GEO", Role.IDENTIFIER, String.class,
                    "POPULATION", Role.MEASURE, Long.class
            ));

            when(right.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "TIME", Role.IDENTIFIER, String.class,
                    "GEO", Role.IDENTIFIER, String.class,
                    "AGE", Role.IDENTIFIER, String.class,
                    "POPULATION", Role.MEASURE, Long.class
            ));

            DataStructure ld = left.getDataStructure();
            when(left.getData()).thenReturn(
                    Stream.of(
                            tuple(ld.wrap("TIME", "2013"),
                                    ld.wrap("GEO", "Belgium"),
                                    ld.wrap("POPULATION", 5L)),
                            tuple(ld.wrap("TIME", "2013"),
                                    ld.wrap("GEO", "Denmark"),
                                    ld.wrap("POPULATION", 2L)),
                            tuple(ld.wrap("TIME", "2013"),
                                    ld.wrap("GEO", "France"),
                                    ld.wrap("POPULATION", 3L)),
                            tuple(ld.wrap("TIME", "2013"),
                                    ld.wrap("GEO", "Spain"),
                                    ld.wrap("POPULATION", 4L))
                    )
            );

            DataStructure rd = right.getDataStructure();
            when(right.getData()).thenReturn(
                    Stream.of(
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Belgium"),
                                    rd.wrap("AGE", "Total"),
                                    rd.wrap("POPULATION", 10L)),
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Greece"),
                                    rd.wrap("AGE", "Total"),
                                    rd.wrap("POPULATION", 11L)),
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Belgium"),
                                    rd.wrap("AGE", "Y15-24"),
                                    rd.wrap("POPULATION", null)),
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Greece"),
                                    rd.wrap("AGE", "Y15-24"),
                                    rd.wrap("POPULATION", 2L)),
                            tuple(rd.wrap("TIME", "2013"),
                                    rd.wrap("GEO", "Spain"),
                                    rd.wrap("AGE", "Y15-24"),
                                    rd.wrap("POPULATION", 6L))
                    )
            );

            InnerJoinOperation join = new InnerJoinOperation(ImmutableMap.of(
                    "left", left, "right", right
            ));
            SumOperation sumOperation = new SumOperation(
                    tuple -> tuple.get(3), ld,
                    tuple -> tuple.get(3), rd
            );

            softly.assertThat(
                    join.getDataStructure()
            ).as("data structure of the sum operation of %s and %s", left, right)
                    .isNotNull();
            // TODO: Better check.

            DataStructure sumDs = sumOperation.getDataStructureOperator().apply(
                    left.getDataStructure(), right.getDataStructure()
            );
            softly.assertThat(
                    StreamUtils.zip(
                            left.getData(), right.getData(), sumOperation.getTupleOperator()
                    )
            ).as("data tuple of the sum operation of %s and %s", left, right)
                    .containsExactly(
                            tuple(sumDs.wrap("TIME", "2013"),
                                    sumDs.wrap("GEO", "Belgium"),
                                    sumDs.wrap("AGE", "Total"),
                                    sumDs.wrap("POPULATION", 15L)),
                            tuple(sumDs.wrap("TIME", "2013"),
                                    sumDs.wrap("GEO", "Belgium"),
                                    sumDs.wrap("AGE", "Y15-24"),
                                    sumDs.wrap("POPULATION", null)),
                            tuple(sumDs.wrap("TIME", "2013"),
                                    sumDs.wrap("GEO", "Spain"),
                                    sumDs.wrap("AGE", "Y15-24"),
                                    sumDs.wrap("POPULATION", 10L))
                    );

        } finally {
            softly.assertAll();
        }
    }

    /**
     * Test the example 2
     * <p>
     * VTL 1.1 line 2537-2541.
     *
     * @throws Exception
     */
    //@Test
    public void testSumEx2() throws Exception {

        Dataset left = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {
            when(left.getDataStructure()).thenReturn(DataStructure.of(mapper::convertValue,
                    "TIME", Role.IDENTIFIER, String.class,
                    "REF_AREA", Role.IDENTIFIER, String.class,
                    "PARTNER", Role.IDENTIFIER, String.class,
                    "OBS_VALUE", Role.MEASURE, String.class,
                    "OBS_STATUS", Role.ATTRIBUTE, String.class
            ));

            DataStructure ld = left.getDataStructure();
            when(left.getData()).thenReturn(
                    Stream.of(
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "EU25"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 20),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "BG"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 2),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "RO"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 2),
                                    ld.wrap("OBS_STATUS", "D"))
                    )
            );

            InnerJoinOperation join = new InnerJoinOperation(ImmutableMap.of(
                    "left", left
            ));
            SumOperation sumOperation = new SumOperation(tuple -> tuple.get(3), ld, 1);

            softly.assertThat(
                    join.getDataStructure()
            ).as("data structure of the sum operation of %s and 1", left)
                    .isEqualTo(join.getDataStructure());

            DataStructure sumDs = sumOperation.getDataStructure();
            softly.assertThat(
                    join.getData()
            ).as("data of the sum operation of %s and 1", left)
                    .containsExactly(
                            tuple(sumDs.wrap("TIME", "2010"),
                                    sumDs.wrap("REF_AREA", "EU25"),
                                    sumDs.wrap("PARTNER", "CA"),
                                    sumDs.wrap("OBS_VALUE", 21),
                                    sumDs.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    sumDs.wrap("REF_AREA", "BG"),
                                    sumDs.wrap("PARTNER", "CA"),
                                    sumDs.wrap("OBS_VALUE", 3),
                                    sumDs.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    sumDs.wrap("REF_AREA", "RO"),
                                    sumDs.wrap("PARTNER", "CA"),
                                    sumDs.wrap("OBS_VALUE", 3),
                                    sumDs.wrap("OBS_STATUS", "D"))
                    );

        } finally {
            softly.assertAll();
        }
    }

    /**
     * Test the example 2
     * <p>
     * VTL 1.1 line 2537-2541.
     *
     * @throws Exception
     */
    //@Test
    public void testSumEx3() throws Exception {
        Dataset left = mock(Dataset.class);
        Dataset right = mock(Dataset.class);

        SoftAssertions softly = new SoftAssertions();
        try {

            DataStructure ds = DataStructure.of(mapper::convertValue,
                    "TIME", Role.IDENTIFIER, String.class,
                    "REF_AREA", Role.IDENTIFIER, String.class,
                    "PARTNER", Role.IDENTIFIER, String.class,
                    "OBS_VALUE", Role.MEASURE, Long.class,
                    "OBS_STATUS", Role.ATTRIBUTE, String.class
            );

            // Same DS.
            when(left.getDataStructure()).thenReturn(ds);
            when(right.getDataStructure()).thenReturn(ds);

            DataStructure ld = left.getDataStructure();
            when(left.getData()).thenReturn(
                    Stream.of(
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "EU25"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 20L),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "BG"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 2L),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "RO"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 2L),
                                    ld.wrap("OBS_STATUS", "D"))
                    )
            );

            when(right.getData()).thenReturn(
                    Stream.of(
                            tuple(ld.wrap("TIME", "2010"),
                                    ld.wrap("REF_AREA", "EU25"),
                                    ld.wrap("PARTNER", "CA"),
                                    ld.wrap("OBS_VALUE", 10L),
                                    ld.wrap("OBS_STATUS", "D")),
                            tuple(ld.wrap("TIME", "2010"))
                    )
            );

            InnerJoinOperation join = new InnerJoinOperation(ImmutableMap.of(
                    "left", left,
                    "right", right
            ));


            SumOperation sumOperation = new SumOperation(
                    tuple -> tuple.get(3),
                    ld,
                    tuple -> tuple.get(3),
                    ld
            );

            softly.assertThat(
                    sumOperation.getDataStructure()
            ).as("data structure of the sum operation of %s and %s", left, right)
                    .isNotEqualTo(left.getDataStructure());

            DataStructure sumDs = join.getDataStructure();
            softly.assertThat(
                    join.getData()
            ).as("data of the sum operation of %s and %s", left, right)
                    .containsExactly(
                            tuple(sumDs.wrap("TIME", "2010"),
                                    sumDs.wrap("REF_AREA", "EU25"),
                                    sumDs.wrap("PARTNER", "CA"),
                                    sumDs.wrap("OBS_VALUE", 30L))
                    );

        } finally {
            softly.assertAll();
        }
    }

    private DataPoint tuple(VTLObject... components) {
        return DataPoint.create(Arrays.asList(components));
    }
}
