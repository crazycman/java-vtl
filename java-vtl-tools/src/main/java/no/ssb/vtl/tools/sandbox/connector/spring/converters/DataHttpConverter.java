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

    public static final String MEDIA_TYPE_VALUE = "application/ssb.dataset-data+json";
    public static final MediaType MEDIA_TYPE = MediaType.parseMediaType(MEDIA_TYPE_VALUE);

    // @formatter:off
    private static final TypeToken<Stream<DataPoint>> SUPPORTED_TYPE = new TypeToken<Stream<DataPoint>>() {};
    private static final TypeReference<List<Object>> LIST_TYPE_REFERENCE = new TypeReference<List<Object>>() {};
    // @formatter:on

    private final ObjectMapper mapper;

    protected DataHttpConverter(MediaType mediaType, ObjectMapper mapper) {
        super(mediaType);
        this.mapper = checkNotNull(mapper);
    }

    public DataHttpConverter(ObjectMapper mapper) {
        this(MEDIA_TYPE, mapper);
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


    @Override
    protected Stream<DataPoint> readInternal(Class<? extends Stream<DataPoint>> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {

        JsonParser parser = mapper.getFactory().createParser(inputMessage.getBody());

        parser.nextValue();
        MappingIterator<List<Object>> data = mapper.readerFor(LIST_TYPE_REFERENCE)
                .readValues(parser);

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        data, Spliterator.IMMUTABLE
                ), false
        ).map(objects -> DataPoint.create(objects.stream().map(VTLObject::of).collect(Collectors.toList())));
    }


    @Override
    protected void writeInternal(Stream stream, Type type, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        // TODO.
    }


}
