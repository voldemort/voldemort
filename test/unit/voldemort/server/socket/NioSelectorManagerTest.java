package voldemort.server.socket;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.store.SleepyStore;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.socket.clientrequest.GetClientRequest;
import voldemort.utils.ByteArray;


@RunWith(Parameterized.class)
public class NioSelectorManagerTest {

    private int port;
    private ClientRequestExecutorPool pool;
    private AbstractSocketService socketService;
    private RequestHandlerFactory factory;
    private SocketDestination dest1;
    private SleepyStore<ByteArray, byte[], byte[]> sleepyStore;

    private final static String STORE_NAME = "test";

    // This can be never below the selector poll time of 500 ms.
    private final long MAX_HEART_BEAT_MS = 600;
    private final int numSelectors;

    public NioSelectorManagerTest(int numSelectors) {
        this.numSelectors = numSelectors;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { 1 }, { 2 } });
    }

    @Before
    public void setUp() throws IOException {
        this.port = ServerTestUtils.findFreePort();

        StoreRepository repository = new StoreRepository();
        sleepyStore = new SleepyStore<ByteArray, byte[], byte[]>(Long.MAX_VALUE, new InMemoryStorageEngine<ByteArray, byte[], byte[]>(STORE_NAME));
        repository.addLocalStore(sleepyStore);
        repository.addRoutedStore(sleepyStore);
        this.pool = new ClientRequestExecutorPool(50, 300, 250, 32 * 1024);
        this.dest1 = new SocketDestination("localhost", port, RequestFormatType.VOLDEMORT_V1);

        final Store<ByteArray, byte[], byte[]> socketStore = pool.create(STORE_NAME, "localhost", port, RequestFormatType.VOLDEMORT_V1, RequestRoutingType.NORMAL);
        factory = ServerTestUtils.getSocketRequestHandlerFactory(repository);
        socketService = ServerTestUtils.getSocketService(true, factory, port, numSelectors, 10, 1000, MAX_HEART_BEAT_MS);
        socketService.start();
    }

    @After
    public void tearDown() throws IOException {
        this.pool.close();
        this.socketService.stop();
    }

    private GetClientRequest getClientRequest() {
        GetClientRequest clientRequest =
                new GetClientRequest(STORE_NAME,
                                     new RequestFormatFactory().getRequestFormat(dest1.getRequestFormatType()),
                                     RequestRoutingType.ROUTED,
                                     new ByteArray(new byte[] { 1, 2, 3 }),
                                     null);

        return clientRequest;
    }

    private NonblockingStoreCallback getCallBack() {
        final AtomicInteger cancelledEvents = new AtomicInteger(0);

        NonblockingStoreCallback callback = new NonblockingStoreCallback() {

            @Override
            public void requestComplete(Object result, long requestTime) {
                if (result instanceof UnreachableStoreException)
                    cancelledEvents.incrementAndGet();
                else
                    fail("The request must have failed with UnreachableException" + result);
            }
        };
        return callback;
    }

    @Test
    public void testSelectorHeartBeat() throws Exception {
        // Open a request so that Sleepy store hangs.
        pool.submitAsync(dest1, getClientRequest(), getCallBack(), 250, "get");

        // wait for MAX heart beat + some more time so that the selector
        // threads should be marked dead
        Thread.sleep(MAX_HEART_BEAT_MS + 50);

        // submitAsync creates two connections if no connection is available when 
        // the request is submitted. First when trying to get a connection for the
        // request, if not it calls proceesLoop which creates one more.
        try {
            pool.checkout(dest1);
        } catch (Exception ex) {

        }

        // If the server is started with single selector, all the requests to the server should
        // fail with IOException as the Server should close the connection because of not
        // receiving the heart beats

        // If the server is started with more than one selector, the requests should succeed
        // and should not be assigned to the hung selector.

        // Note that, this is just checking out a connection and not doing any operation on it
        // If an operation is attempted, the operation will time out as the store being used
        // here sleeps for infinite time
        for (int i = 0; i < 20; i++) {
            try {
                ClientRequestExecutor executor = pool.checkout(dest1);
                if (numSelectors == 1) {
                    fail("Single selector expected an exception");
                } else {
                    assertNotNull("multiple selector should have valid executor", executor);
                }
            } catch (UnreachableStoreException ex) {
                if (numSelectors > 1) {
                    fail("more than one selector, no exception should be observed" + ex);
                } else {
                    assertTrue("inner exception should be instance of IOException", ex.getCause() instanceof IOException);
                }
            }
        }

        sleepyStore.releaseThreads();
    }
}
