package no.ssb.vtl.tools.sandbox.connector.spring.converters;

import com.fasterxml.jackson.databind.JavaType;
import com.google.common.annotations.VisibleForTesting;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by hadrien on 15/06/2017.
 */
class SsbDatasetHttpMessageConverter extends MappingJackson2HttpMessageConverter {

    public static final String APPLICATION_DATASET_JSON_VALUE = "application/ssb.dataset+json";
    public static final MediaType APPLICATION_DATASET_JSON = MediaType.parseMediaType(APPLICATION_DATASET_JSON_VALUE);

    // TODO: remove
    public static final String APPLICATION_DATASET_DATA_JSON_VALUE = "application/ssb.dataset.data+json";
    public static final MediaType APPLICATION_DATASET_DATA_JSON = MediaType.parseMediaType(APPLICATION_DATASET_DATA_JSON_VALUE);

    // TODO: remove
    public static final String APPLICATION_DATASET_STRUCTURE_JSON_VALUE = "application/ssb.dataset.structure+json";
    public static final MediaType APPLICATION_DATASET_STRUCTURE_JSON = MediaType.parseMediaType(APPLICATION_DATASET_STRUCTURE_JSON_VALUE);

    @VisibleForTesting
    static final List<MediaType> SUPPORTED_TYPES;

    static {
        SUPPORTED_TYPES = new ArrayList<>();
        SUPPORTED_TYPES.add(APPLICATION_DATASET_JSON);
        SUPPORTED_TYPES.add(APPLICATION_DATASET_DATA_JSON);
        SUPPORTED_TYPES.add(APPLICATION_DATASET_STRUCTURE_JSON);
    }

    @Override
    public Object read(Type type, Class<?> contextClass, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        JavaType javaType = getJavaType(type, contextClass);
        return readInternal(javaType.getRawClass(), inputMessage);
    }

    @Override
    public boolean canRead(Type type, Class<?> contextClass, MediaType mediaType) {
        return canRead(getJavaType(type, contextClass).getRawClass(), mediaType);
    }

    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return canRead(mediaType) && isClassSupported(clazz);
    }

    private boolean isClassSupported(Class<?> clazz) {
        return clazz.isAssignableFrom(DataStructure.class) ||
                clazz.isAssignableFrom(Stream.class) ||
                clazz.isAssignableFrom(Dataset.class);
    }

    @Override
    public boolean canWrite(Class<?> clazz, MediaType mediaType) {
        return canWrite(mediaType) && isClassSupported(clazz);
    }

    @Override
    public List<MediaType> getSupportedMediaTypes() {
        return SUPPORTED_TYPES;
    }

    @Override
    protected Object readInternal(Class<?> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        return super.readInternal(clazz, inputMessage);
    }

    @Override
    protected void writeInternal(Object object, Type type, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        super.writeInternal(object, type, outputMessage);
    }
}
