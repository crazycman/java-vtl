package no.ssb.vtl.tools.sandbox.connector.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.vtl.model.DataStructure;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Spring {@link org.springframework.http.converter.HttpMessageConverter} for {@link DataStructure}.
 */
public class DataStructureHttpConverter extends AbstractHttpMessageConverter<DataStructure> {

    public static final String MEDIA_TYPE_VALUE = "application/ssb.dataset.structure+json";
    public static final MediaType MEDIA_TYPE = MediaType.parseMediaType(MEDIA_TYPE_VALUE);

    private final ObjectMapper mapper;

    protected DataStructureHttpConverter(MediaType supportedMediaType, ObjectMapper mapper) {
        super(supportedMediaType);
        this.mapper = checkNotNull(mapper);
    }

    public DataStructureHttpConverter(ObjectMapper mapper) {
        this(MEDIA_TYPE, mapper);
    }

    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return clazz.isAssignableFrom(DataStructure.class) && canRead(mediaType);
    }

    @Override
    public boolean canWrite(Class<?> clazz, MediaType mediaType) {
        return DataStructure.class.isAssignableFrom(clazz) && canWrite(mediaType);
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        throw new UnsupportedOperationException(); // we rely on can read and can write.
    }

    @Override
    protected DataStructure readInternal(Class<? extends DataStructure> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        return null;
    }

    @Override
    protected void writeInternal(DataStructure dataStructure, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {

    }
}
