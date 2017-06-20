package no.ssb.vtl.tools.sandbox.connector.spring.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Before;
import org.junit.Rule;
import org.springframework.http.MediaType;

import java.util.Set;
import java.util.function.BiFunction;

import static no.ssb.vtl.tools.sandbox.connector.spring.converters.DatasetHttpMessageConverter.SUPPORTED_TYPES;

public class DatasetHttpMessageConverterTest {

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    private DatasetHttpMessageConverter converter;

    private Set<MediaType> unsupportedType = ImmutableSet.of(
            MediaType.parseMediaType("something/else"),
            MediaType.parseMediaType("application/json")
    );

    private Set<MediaType> supportedTypes = ImmutableSet.copyOf(
            SUPPORTED_TYPES
    );

    @Before
    public void setUp() throws Exception {
        converter = new DatasetHttpMessageConverter(new ObjectMapper());
    }

    private static class ExtendedDataStructure extends DataStructure {
        protected ExtendedDataStructure(BiFunction<Object, Class<?>, ?> converter, ImmutableMap<String, Component> map) {
            super(converter, map);
        }
    }
}
