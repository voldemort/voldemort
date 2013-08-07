package voldemort.restclient;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.codec.binary.Base64;

import voldemort.rest.RestMessageHeaders;
import voldemort.rest.RestUtils;
import voldemort.versioning.VectorClock;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;

public class MicroBenchmarkR2 {

    private static RestRequest createRequest() throws URISyntaxException {
        RestRequestBuilder rb = new RestRequestBuilder(new URI("http://localhost:8080/test/asfasdf"));
        // Create a HTTP POST request
        rb.setMethod("DELETE");
        rb.setHeader("Content-Length", "0");
        String timeoutStr = "1500";
        rb.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
        rb.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                     String.valueOf(System.currentTimeMillis()));
        RestRequest request = rb.build();
        return request;
    }

    private static String encodeBase64(byte[] bytes) {
        String base64Key = new String(Base64.encodeBase64(bytes));
        return base64Key;
    }

    private static byte[] decodeBase64(String base64Key) {
        byte[] bytes = Base64.decodeBase64(base64Key);
        return bytes;
    }

    private static String serializeVC(VectorClock vc) {
        return RestUtils.getSerializedVectorClock(vc);
    }

    private static VectorClock deSerializeVC(String serializedVC) {
        return RestUtils.deserializeVectorClock(serializedVC);
    }

    private static void performOp() throws URISyntaxException {
        HttpClientFactory _clientFactory = new HttpClientFactory();
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(HttpClientFactory.POOL_SIZE_KEY, "1");
        TransportClient transportClient = _clientFactory.getClient(properties);
        Client client = new TransportClientAdapter(transportClient);

        // New connection
        Future<RestResponse> newConnectionRequest = client.restRequest(createRequest());

        // Checkout of existing connection
        Future<RestResponse> reuseConnectionRequest = client.restRequest(createRequest());

        final FutureCallback<None> clientShutdownCallback = new FutureCallback<None>();
        client.shutdown(clientShutdownCallback);
        try {
            clientShutdownCallback.get();
        } catch(InterruptedException e) {
            System.err.println("Interrupted while shutting down the HttpClientFactory: "
                               + e.getMessage());
        } catch(ExecutionException e) {
            System.err.println("Execution exception occurred while shutting down the HttpClientFactory: "
                               + e.getMessage());
        }

        final FutureCallback<None> factoryShutdownCallback = new FutureCallback<None>();
        _clientFactory.shutdown(factoryShutdownCallback);
        try {
            factoryShutdownCallback.get();
        } catch(InterruptedException e) {
            System.err.println("Interrupted while shutting down the HttpClientFactory: "
                               + e.getMessage());
        } catch(ExecutionException e) {
            System.err.println("Execution exception occurred while shutting down the HttpClientFactory: "
                               + e.getMessage());
        }

    }

    private static VectorClock getRandomVectorClock(int numNodes) {
        VectorClock clock = new VectorClock();
        for(int i = 0; i < numNodes; i++) {
            clock.incrementVersion((short) i, System.currentTimeMillis());
        }
        return clock;
    }

    /**
     * @param args
     * @throws URISyntaxException
     * @throws InterruptedException
     */

    public static void main(String[] args) throws URISyntaxException, InterruptedException {
        // for(int i = 0; i < 100; i++) {
        performOp();
        // Thread.sleep(2000);
        // }
    }

    public static void main2(String[] args) throws URISyntaxException, InterruptedException {
        long totalDiffTimeInNs = 0;
        int numBytes = 10000;
        int numNodes = 100;
        long totalVCSerializationCost = 0;
        long totalVCDeSerializationCost = 0;

        System.out.println("#Nodes = " + numNodes);

        for(int i = 0; i < 100; i++) {

            // byte[] b = new byte[numBytes];
            // new Random().nextBytes(b);
            //
            // long startTimeNs = System.nanoTime();
            // String encodedKey = encodeBase64(b);
            // long diffInNs = System.nanoTime() - startTimeNs;
            // System.out.println("Time taken to encode: " + diffInNs +
            // " (ns) ");
            //
            // startTimeNs = System.nanoTime();
            // decodeBase64(encodedKey);
            // diffInNs = System.nanoTime() - startTimeNs;
            // System.out.println("Time taken to decode: " + diffInNs +
            // " (ns) ");

            VectorClock vc = getRandomVectorClock(numNodes);
            long startTimeNs = System.nanoTime();
            String serialedVC = serializeVC(vc);
            long diffInNs = System.nanoTime() - startTimeNs;
            System.out.println("Time taken to serialize VC  : " + diffInNs + " (ns) ");
            if(i != 0) {
                totalVCSerializationCost += diffInNs;
            }

            startTimeNs = System.nanoTime();
            deSerializeVC(serialedVC);
            diffInNs = System.nanoTime() - startTimeNs;
            System.out.println("Time taken to deserialize VC: " + diffInNs + " (ns) ");
            if(i != 0) {
                totalVCDeSerializationCost += diffInNs;
            }

            Thread.sleep(100);
        }

        System.out.println("Average time to serialize a VC : " + totalVCSerializationCost / 99
                           + " (ns)");

        System.out.println("Average time to de-serialize a VC : " + totalVCDeSerializationCost / 99
                           + " (ns)");

    }
}
