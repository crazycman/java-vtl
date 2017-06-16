package no.ssb.vtl.tools.sandbox.connector.spring.converters;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.assertj.core.api.JUnitSoftAssertions;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.http.MediaType;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static no.ssb.vtl.tools.sandbox.connector.spring.converters.SsbDatasetHttpMessageConverter.APPLICATION_DATASET_DATA_JSON;
import static no.ssb.vtl.tools.sandbox.connector.spring.converters.SsbDatasetHttpMessageConverter.APPLICATION_DATASET_JSON;
import static no.ssb.vtl.tools.sandbox.connector.spring.converters.SsbDatasetHttpMessageConverter.APPLICATION_DATASET_STRUCTURE_JSON;
import static no.ssb.vtl.tools.sandbox.connector.spring.converters.SsbDatasetHttpMessageConverter.SUPPORTED_TYPES;

public class SsbDatasetHttpMessageConverterTest {

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    private SsbDatasetHttpMessageConverter converter;

    private Set<MediaType> unsupportedType = ImmutableSet.of(
            MediaType.parseMediaType("something/else"),
            MediaType.parseMediaType("application/json")
    );

    private Set<MediaType> supportedTypes = ImmutableSet.copyOf(
            SUPPORTED_TYPES
    );

    @Before
    public void setUp() throws Exception {
        converter = new SsbDatasetHttpMessageConverter();
    }

    @Test
    public void supportReadingDataStructure() throws Exception {

        softly.assertThat(

                converter.canRead(DataStructure.class, APPLICATION_DATASET_STRUCTURE_JSON)

        ).as(
                "supports reading class %s with media type %s",
                DataStructure.class,
                APPLICATION_DATASET_STRUCTURE_JSON
        ).isTrue();

        softly.assertThat(

                converter.canRead(ExtendedDataStructure.class, APPLICATION_DATASET_STRUCTURE_JSON)

        ).as(
                "supports reading subclass %s with media type %s",
                ExtendedDataStructure.class,
                APPLICATION_DATASET_STRUCTURE_JSON
        ).isFalse();

        // Assert wrong media types.
        List<MediaType> invalidTypes = Lists.newArrayList();
        invalidTypes.addAll(unsupportedType);
        invalidTypes.addAll(
                Sets.difference(supportedTypes, Collections.singleton(APPLICATION_DATASET_STRUCTURE_JSON))
        );
        for (MediaType invalidType : invalidTypes) {
            softly.assertThat(converter.canRead(DataStructure.class, APPLICATION_DATASET_STRUCTURE_JSON))
                    .as(
                            "supports reading class %s with media type %s",
                            DataStructure.class,
                            invalidType
                    ).isFalse();
        }
    }

    @Test
    public void supportWritingDataStructure() throws Exception {

        softly.assertThat(

                converter.canWrite(DataStructure.class, APPLICATION_DATASET_STRUCTURE_JSON)

        ).as(
                "supports writing class %s with media type %s",
                DataStructure.class,
                APPLICATION_DATASET_STRUCTURE_JSON
        ).isTrue();

        softly.assertThat(

                converter.canWrite(ExtendedDataStructure.class, APPLICATION_DATASET_STRUCTURE_JSON)

        ).as(
                "supports writing subclass %s with media type %s",
                ExtendedDataStructure.class,
                APPLICATION_DATASET_STRUCTURE_JSON
        ).isTrue();

        // Assert wrong media types.
        List<MediaType> invalidTypes = Lists.newArrayList();
        invalidTypes.addAll(unsupportedType);
        invalidTypes.addAll(
                Sets.difference(supportedTypes, Collections.singleton(APPLICATION_DATASET_STRUCTURE_JSON))
        );
        for (MediaType invalidType : invalidTypes) {
            softly.assertThat(

                    converter.canRead(DataStructure.class, APPLICATION_DATASET_STRUCTURE_JSON)

            ).as(
                    "supports writing class %s with media type %s",
                    DataStructure.class,
                    invalidType
            ).isFalse();
        }

    }

    @Test
    public void supportReadingDataset() throws Exception {

        softly.assertThat(

                converter.canRead(Dataset.class, APPLICATION_DATASET_JSON)

        ).as(
                "supports reading class %s with media type %s",
                Dataset.class,
                APPLICATION_DATASET_JSON
        ).isTrue();

        softly.assertThat(

                converter.canRead(ExtendedDataset.class, APPLICATION_DATASET_JSON)

        ).as(
                "supports reading subclass %s with media type %s",
                ExtendedDataset.class,
                APPLICATION_DATASET_JSON
        ).isFalse();

        // Assert wrong media types.
        List<MediaType> invalidTypes = Lists.newArrayList();
        invalidTypes.addAll(unsupportedType);
        invalidTypes.addAll(
                Sets.difference(supportedTypes, Collections.singleton(APPLICATION_DATASET_JSON))
        );
        for (MediaType invalidType : invalidTypes) {
            softly.assertThat(converter.canRead(DataStructure.class, APPLICATION_DATASET_JSON))
                    .as(
                            "supports reading class %s with media type %s",
                            DataStructure.class,
                            invalidType
                    ).isFalse();
        }
    }

    @Test
    public void supportWritingDataset() throws Exception {

        softly.assertThat(

                converter.canWrite(Dataset.class, APPLICATION_DATASET_JSON)

        ).as(
                "supports writing class %s with media type %s",
                Dataset.class,
                APPLICATION_DATASET_JSON
        ).isTrue();

        softly.assertThat(

                converter.canWrite(ExtendedDataset.class, APPLICATION_DATASET_JSON)

        ).as(
                "supports writing subclass %s with media type %s",
                Dataset.class,
                APPLICATION_DATASET_JSON
        ).isTrue();

        // Assert wrong media types.
        List<MediaType> invalidTypes = Lists.newArrayList();
        invalidTypes.addAll(unsupportedType);
        invalidTypes.addAll(
                Sets.difference(supportedTypes, Collections.singleton(APPLICATION_DATASET_JSON))
        );
        for (MediaType invalidType : invalidTypes) {
            softly.assertThat(

                    converter.canRead(Dataset.class, APPLICATION_DATASET_JSON)

            ).as(
                    "supports writing class %s with media type %s",
                    Dataset.class,
                    invalidType
            ).isFalse();
        }

    }

    @Test
    public void supportReadingData() throws Exception {

        TypeToken<Stream<DataPoint>> typeToken = new TypeToken<Stream<DataPoint>>() {
        };
        Type type = typeToken.getType();

        softly.assertThat(

                converter.canRead(type, this.getClass(), APPLICATION_DATASET_DATA_JSON)

        ).as(
                "supports reading class %s with media type %s should succeed",
                type,
                APPLICATION_DATASET_DATA_JSON
        ).isTrue();

        TypeToken<ExtendedData> extendedTypeToken = new TypeToken<ExtendedData>() {
        };
        Type extendedType = extendedTypeToken.getType();

        softly.assertThat(

                converter.canRead(extendedType, this.getClass(), APPLICATION_DATASET_DATA_JSON)

        ).as(
                "supports reading subclass %s with media type %s should succeed",
                extendedType,
                APPLICATION_DATASET_DATA_JSON
        ).isFalse();

        // Assert wrong media types.
        List<MediaType> invalidTypes = Lists.newArrayList();
        invalidTypes.addAll(unsupportedType);
        invalidTypes.addAll(
                Sets.difference(supportedTypes, Collections.singleton(APPLICATION_DATASET_DATA_JSON))
        );
        for (MediaType invalidType : invalidTypes) {
            softly.assertThat(

                    converter.canRead(type, this.getClass(), APPLICATION_DATASET_DATA_JSON)

            ).as(
                    "supports reading class %s with media type %s should fail",
                    type,
                    invalidType
            ).isFalse();
        }

    }

    @Test
    public void supportWritingData() throws Exception {

        TypeToken<Stream<DataPoint>> typeToken = new TypeToken<Stream<DataPoint>>() {
        };
        Type type = typeToken.getType();

        softly.assertThat(

                converter.canWrite(type, this.getClass(), APPLICATION_DATASET_DATA_JSON)

        ).as(
                "supports writing class %s with media type %s should succeed",
                type,
                APPLICATION_DATASET_JSON
        ).isTrue();

        TypeToken<ExtendedData> extendedTypeToken = new TypeToken<ExtendedData>() {
        };
        Type extendedType = extendedTypeToken.getType();

        softly.assertThat(

                converter.canWrite(extendedType, this.getClass(), APPLICATION_DATASET_DATA_JSON)

        ).as(
                "supports writing subclass %s with media type %s should succeed",
                extendedType,
                APPLICATION_DATASET_JSON
        ).isFalse();

        // Assert wrong media types.
        List<MediaType> invalidTypes = Lists.newArrayList();
        invalidTypes.addAll(unsupportedType);
        invalidTypes.addAll(
                Sets.difference(supportedTypes, Collections.singleton(APPLICATION_DATASET_DATA_JSON))
        );
        for (MediaType invalidType : invalidTypes) {
            softly.assertThat(

                    converter.canWrite(type, this.getClass(), APPLICATION_DATASET_DATA_JSON)

            ).as(
                    "supports writing class %s with media type %s should fail",
                    type,
                    invalidType
            ).isFalse();
        }

    }

    private interface ExtendedData extends Stream<DataPoint> {
    }

    private interface ExtendedDataset extends Dataset {

    }

    private static class ExtendedDataStructure extends DataStructure {
        protected ExtendedDataStructure(BiFunction<Object, Class<?>, ?> converter, ImmutableMap<String, Component> map) {
            super(converter, map);
        }
    }
}
