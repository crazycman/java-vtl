package no.ssb.vtl.tools.sandbox.connector.spring;

import org.junit.Test;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hadrien on 13/06/17.
 */
public class RestClientConnectorTest {

    @Test
    public void getData() throws Exception {
        SimpleClientHttpRequestFactory schrf = new SimpleClientHttpRequestFactory();
        schrf.setBufferRequestBody(false);
        schrf.setTaskExecutor(new SimpleAsyncTaskExecutor());

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        RestClientConnector restClientConnector = new RestClientConnector(
                new RestTemplate(schrf),
                executorService
        );

        restClientConnector.getDataWithExecutor();
    }

}
