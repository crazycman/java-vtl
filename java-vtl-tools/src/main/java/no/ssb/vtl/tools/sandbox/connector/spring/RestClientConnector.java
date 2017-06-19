package no.ssb.vtl.tools.sandbox.connector.spring;

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
import org.springframework.web.client.HttpMessageConverterExtractor;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A connector that relies on {@link RestTemplate}.
 * <p>
 * In order to allow streaming the requests needs to be started in another
 * thread because RestTemplate will close the connection.
 * <p>
 * This class solves this issue by executing the requests in a thread pools and
 * transfer the result to a wrapped stream.
 */
public class RestClientConnector {

    // End of stream marker.
    private static final DataPoint EOS = DataPoint.create(0);

    private final ExecutorService executorService;
    private final WrappedRestTemplate template;

    public RestClientConnector(RestTemplate template, ExecutorService executorService) {
        this.template = new WrappedRestTemplate(checkNotNull(template));
        this.executorService = checkNotNull(executorService);
    }

    public void test() throws ExecutionException, InterruptedException {

        try {
            ResponseEntity<DataStructure> structure = template.getForEntity("http://dataset", DataStructure.class);
        } catch (RestClientException rce) {
            rce.printStackTrace();
        }

        try {
            ResponseEntity<Dataset> forEntity = template.getForEntity("http://dataset", Dataset.class);
        } catch (RestClientException rce) {
            rce.printStackTrace();
        }
    }

    /**
     * Create a new data stream by starting a request in another thread.
     *
     * @return
     */
    private Stream<DataPoint> createDataStream(URI uri) {


        ResponseEntity<Dataset> dataset = template.getForEntity(uri, Dataset.class);

        return null;
    }

    public void getDataWithExecutor() throws IOException, InterruptedException {

// Does not work, stream is closed.
//
//        ResponseEntity<InputStreamResource> forEntity = template.getForEntity(
//                URI.create("http://www.mocky.io/v2/5940200d100000f410cd122c"),
//                InputStreamResource.class
//        );
//        ByteStreams.copy(forEntity.getBody().getInputStream(), System.out);
//        System.out.println(forEntity.getBody());


        template.execute(
                URI.create("http://www.mocky.io/v2/5940200d100000f410cd122c"),
                HttpMethod.GET,
                new RequestCallback() {
                    @Override
                    public void doWithRequest(ClientHttpRequest request) throws IOException {
                        template.acceptHeaderRequestCallback(Dataset.class).doWithRequest(request);
                        System.out.println("Got request" + request);
                    }
                },
                new ResponseExtractor<Void>() {
                    @Override
                    public Void extractData(ClientHttpResponse response) throws IOException {

                        HttpMessageConverterExtractor<List> extractor = new HttpMessageConverterExtractor<>(
                                List.class, template.getMessageConverters()
                        );
                        List dataset = extractor.extractData(response);

                        //dataset.getData().forEach(System.out::println);

                        return null;
                    }
                }
        );

// Works
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

        ObjectMapper mapper = new ObjectMapper();
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

    /**
     * Exposes some of the variables.
     */
    public class WrappedRestTemplate extends RestTemplate {

        public WrappedRestTemplate(RestTemplate template) {

            // TODO(hk): Check if this has consequences.
            // setDefaultUriVariables();

            setErrorHandler(template.getErrorHandler());
            setInterceptors(template.getInterceptors());
            setMessageConverters(template.getMessageConverters());
            setRequestFactory(template.getRequestFactory());
            setUriTemplateHandler(getUriTemplateHandler());
        }

        @Override
        public  <T> RequestCallback acceptHeaderRequestCallback(Class<T> responseType) {
            return super.acceptHeaderRequestCallback(responseType);
        }

        @Override
        protected <T> ResponseExtractor<ResponseEntity<T>> responseEntityExtractor(Type responseType) {
            return super.responseEntityExtractor(responseType);
        }
    }


}
