package no.ssb.vtl.tools.sandbox.connector;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLObject;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.GenericHttpMessageConverter;
import org.springframework.web.client.AsyncRestTemplate;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A connector that relies on {@link RestTemplate}
 */
public class RestClientConnector {

    // End of stream.
    private static final DataPoint EOS = DataPoint.create(0);

    final AsyncRestTemplate asyncTemplate;
    /*
     * Sadly, spring gives very little control over how it processes the
     * responses' bodies of the requests made by the RestTemplate.
     * That is why we need an executor service to copy the data from the
     * active connections to the streams.
     */
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    RestTemplate template = new RestTemplate();
    private ObjectMapper mapper = new ObjectMapper();

    public RestClientConnector(AsyncRestTemplate template) {
        this.asyncTemplate = checkNotNull(template);
    }

    public void test() throws ExecutionException, InterruptedException {

        try {
            ResponseEntity<DataStructure> structure = asyncTemplate.getForEntity("http://dataset", DataStructure.class).get();
        } catch (RestClientException rce) {
            rce.printStackTrace();
        }

        try {
            ResponseEntity<Dataset> forEntity = asyncTemplate.getForEntity("http://dataset", Dataset.class).get();
        } catch (RestClientException rce) {
            rce.printStackTrace();
        }
    }

    public void getDataWithExecutor() throws IOException, InterruptedException {

        ClientHttpRequestFactory factory = template.getRequestFactory();
        ClientHttpRequest request = factory.createRequest(
                URI.create("http://www.mocky.io/v2/5940200d100000f410cd122c"),
                HttpMethod.GET
        );

        final BlockingQueue<DataPoint> queue = Queues.newArrayBlockingQueue(100);
        final Thread reader = Thread.currentThread();

        ClientHttpResponse response = request.execute();
        Runnable task = createExtractingTask(response, queue, reader);
        Future<?> future = executorService.submit(task);

        Spliterator<DataPoint> spliterator = new Spliterators.AbstractSpliterator<DataPoint>(
                Long.MAX_VALUE,
                Spliterator.IMMUTABLE
        ) {

            @Override
            public boolean tryAdvance(Consumer<? super DataPoint> action) {
                try {

                    DataPoint p = queue.take();
                    if (p == EOS)
                        return false;

                    action.accept(p);

                } catch (InterruptedException ie) {
                    future.cancel(true);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("stream interrupted");
                }
                return true;
            }

            @Override
            public void forEachRemaining(Consumer<? super DataPoint> action) {
                try {

                    DataPoint p;
                    while ((p = queue.take()) != EOS)
                        action.accept(p);

                } catch (InterruptedException ie) {
                    future.cancel(true);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("stream interrupted");
                }
            }
        };

        Stream<DataPoint> stream = StreamSupport.stream(spliterator, false);

        stream.forEach(System.out::println);

    }

    private Runnable createExtractingTask(
            final ClientHttpResponse response,
            final BlockingQueue<DataPoint> queue,
            final Thread reader) {
        return () -> {
            try (InputStream body = response.getBody()) {
                MappingIterator<List<?>> it = createMappingIterator(body);
                while (it.hasNext()) {
                    List<?> next = it.next();
                    DataPoint dp = DataPoint.create(Lists.transform(next, VTLObject::of));
                    queue.put(dp);
                }
                queue.put(EOS);

            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
                reader.interrupt();
            }
        };
    }

    private MappingIterator<List<?>> createMappingIterator(InputStream response) throws IOException {
        TypeReference<List<?>> reference;
        reference = new TypeReference<List<?>>() {
        };

        JsonParser parser = mapper.getFactory().createParser(response);
        parser.nextToken();
        parser.nextToken();
        return mapper.readValues(
                parser,
                reference
        );
    }

    private Consumer<ClientHttpResponse> createResponseExtractor(
            final BlockingQueue<DataPoint> queue,
            final Thread reader
    ) {

        GenericHttpMessageConverter<Iterator<DataPoint>> dataPointConverter = null;
        Type type = null;

        return clientHttpResponse -> {

            try {

                Iterator<DataPoint> iterator = dataPointConverter.read(
                        type,
                        this.getClass(),
                        clientHttpResponse
                );

                while (iterator.hasNext()) {
                    DataPoint next = iterator.next();
                    queue.put(next);
                }
                queue.put(null);
            } catch (InterruptedException e) {
                reader.interrupt();
                Thread.currentThread().interrupt();
                // TODO: Log debug.
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
    }

}
