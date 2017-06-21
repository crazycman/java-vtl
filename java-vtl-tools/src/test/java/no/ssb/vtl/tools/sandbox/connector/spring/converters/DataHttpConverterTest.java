package no.ssb.vtl.tools.sandbox.connector.spring.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.reflect.TypeToken;
import no.ssb.vtl.model.DataPoint;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpInputMessage;
import org.springframework.mock.http.MockHttpInputMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;

/**
 * Created by hadrien on 15/06/2017.
 */
public class DataHttpConverterTest {

    private ObjectMapper mapper;
    private DataHttpConverter converter;

    @Before
    public void setUp() throws Exception {

        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        converter = new DataHttpConverter(mapper);
    }

    @Test
    public void testReadDataVersion2() throws Exception {
        TypeToken<Stream<DataPoint>> typeToken = new TypeToken<Stream<DataPoint>>() {
        };

        HttpInputMessage message = loadFile("ssb.dataset.data+json;version=2.json");

        Stream<DataPoint> result = converter.read(typeToken.getType(), null, message);
        result.forEach(System.out::println);
    }

    private HttpInputMessage loadFile(String name) throws IOException {
        InputStream stream = getResource(name).openStream();
        return new MockHttpInputMessage(stream);
    }
}
