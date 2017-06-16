package no.ssb.vtl.tools.sandbox.connector;

import org.junit.Test;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.client.*;
import org.springframework.web.client.AsyncRestTemplate;

/**
 * Created by hadrien on 13/06/17.
 */
public class RestClientConnectorTest {

    @Test
    public void getData() throws Exception {
        SimpleClientHttpRequestFactory schrf = new SimpleClientHttpRequestFactory();
        schrf.setBufferRequestBody(false);
        schrf.setTaskExecutor(new SimpleAsyncTaskExecutor());
        AsyncClientHttpRequestFactory factory = schrf;

        AsyncRestTemplate aSyncTemplate = new AsyncRestTemplate(factory);

        RestClientConnector restClientConnector = new RestClientConnector(aSyncTemplate);

        restClientConnector.getDataWithExecutor();
    }

}