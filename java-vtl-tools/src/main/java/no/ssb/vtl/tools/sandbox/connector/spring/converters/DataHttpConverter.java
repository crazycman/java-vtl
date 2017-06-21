package no.ssb.vtl.tools.sandbox.connector.spring.converters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.VTLObject;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by hadrien on 15/06/2017.
 */
public class DataHttpConverter extends AbstractGenericHttpMessageConverter<Stream<DataPoint>> {

    public static final String APPLICATION_SSB_DATASET_DATA_JSON_V1_VALUE = "application/ssb.dataset-data+json;version=1";
    public static final String APPLICATION_SSB_DATASET_DATA_JSON_V2_VALUE = "application/ssb.dataset-data+json;version=2";

    public static final MediaType APPLICATION_SSB_DATASET_DATA_JSON_V1 = MediaType.parseMediaType(APPLICATION_SSB_DATASET_DATA_JSON_V1_VALUE);
    public static final MediaType APPLICATION_SSB_DATASET_DATA_JSON_V2 = MediaType.parseMediaType(APPLICATION_SSB_DATASET_DATA_JSON_V2_VALUE);


    // @formatter:off
    private static final TypeToken<Stream<DataPoint>> SUPPORTED_TYPE = new TypeToken<Stream<DataPoint>>() {};
    private static final TypeReference<List<VTLObjectWrapper>> LIST_TYPE_REFERENCE = new TypeReference<List<VTLObjectWrapper>>() {};
    // @formatter:on

    private final ObjectMapper mapper;

    public DataHttpConverter(ObjectMapper mapper) {
        super(
                APPLICATION_SSB_DATASET_DATA_JSON_V1,
                APPLICATION_SSB_DATASET_DATA_JSON_V2
        );
        this.mapper = checkNotNull(mapper);
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        throw new UnsupportedOperationException();
    }

    /**
     * @see DataHttpConverter#canRead(TypeToken, MediaType)
     */
    @Override
    public boolean canWrite(Type type, Class<?> clazz, MediaType mediaType) {
        return super.canWrite(type, clazz, mediaType);
    }

    /**
     * @see DataHttpConverter#canRead(TypeToken, MediaType)
     */
    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return canRead(TypeToken.of(clazz), mediaType);
    }

    /**
     * @see DataHttpConverter#canRead(TypeToken, MediaType)
     */
    @Override
    public boolean canRead(Type type, Class<?> contextClass, MediaType mediaType) {
        // TODO: Maybe use context?
        return canRead(TypeToken.of(type), mediaType);
    }

    /**
     * @see #canRead(Type, Class, MediaType)
     * @see #canRead(Class, MediaType)
     */
    private boolean canRead(TypeToken<?> token, MediaType mediaType) {
        return token.isSubtypeOf(SUPPORTED_TYPE) && canRead(mediaType);
    }

    @Override
    public Stream<DataPoint> read(Type type, Class<?> contextClass, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        return readInternal(null, inputMessage);
    }

    Stream<DataPoint> readWithParser(JsonParser parser) throws IOException {

        MappingIterator<List<VTLObjectWrapper>> data = mapper.readerFor(LIST_TYPE_REFERENCE)
                .readValues(parser);

        Stream<List<VTLObjectWrapper>> rawStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        data, Spliterator.IMMUTABLE
                ), false
        );

        return rawStream.map(pointWrappers -> {
            return pointWrappers.stream()
                    .map(this::toVTLObject)
                    .collect(Collectors.toList()
                    );
        }).map(DataPoint::create);
    }

    @Override
    protected Stream<DataPoint> readInternal(Class<? extends Stream<DataPoint>> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        // TODO: wrap exceptions in HttpMessageNotReadableException
        JsonParser parser = mapper.getFactory().createParser(inputMessage.getBody());
        parser.nextValue();
        parser.nextValue();
        return readWithParser(parser);
    }


    @Override
    protected void writeInternal(Stream stream, Type type, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        // TODO.
    }

    private VTLObject toVTLObject(VTLObjectWrapper VTLObjectWrapper) {
        if (VTLObjectWrapper == null || VTLObjectWrapper.val == null)
            return VTLObject.NULL;
        return VTLObject.of(
                mapper.convertValue(VTLObjectWrapper.val, VTLObjectWrapper.type.getType())
        );
    }

    private static class VTLObjectWrapper {
        private RoleMapping type;
        private Object val;

        public RoleMapping getType() {
            return type;
        }

        public void setType(RoleMapping type) {
            this.type = type;
        }

        public Object getVal() {
            return val;
        }

        public void setVal(Object val) {
            this.val = val;
        }
    }

}
