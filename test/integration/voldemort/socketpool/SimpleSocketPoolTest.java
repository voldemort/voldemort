package voldemort.socketpool;

import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.protocol.SocketRequestHandlerFactory;
import voldemort.server.socket.SocketServer;
import voldemort.socketpool.AbstractSocketPoolTest.TestStats;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.utils.pool.ResourceFactory;
import voldemort.utils.pool.ResourcePoolConfig;

public class SimpleSocketPoolTest extends TestCase {

    public void testPoolLimitNoTimeout() throws Exception {
        final ResourcePoolConfig config = new ResourcePoolConfig().setTimeout(1000,
                                                                              TimeUnit.MILLISECONDS)
                                                                  .setMaxPoolSize(20);

        ResourceFactory<String, String> factory = ResourcePoolTestUtils.getBasicPoolFactory();
        final AbstractSocketPoolTest<String, String> test = new AbstractSocketPoolTest<String, String>() {

            @Override
            protected void doSomethingWithResource(String key, String resource) throws Exception {
                Thread.sleep(100);
            }

            @Override
            protected String getRequestKey() throws Exception {
                return "test-key";
            }
        };

        // borrow timeout >> doSomething() no timeout expected
        TestStats testStats = test.startTest(factory, config, 50, 200);
        assertEquals("We should see Zero timeoutRequests", 0, testStats.timeoutRequests);
    }

    public void testPoolLimitSomeTimeout() throws Exception {
        final ResourcePoolConfig config = new ResourcePoolConfig().setTimeout(50,
                                                                              TimeUnit.MILLISECONDS)
                                                                  .setMaxPoolSize(20);

        ResourceFactory<String, String> factory = ResourcePoolTestUtils.getBasicPoolFactory();
        final AbstractSocketPoolTest<String, String> test = new AbstractSocketPoolTest<String, String>() {

            @Override
            protected void doSomethingWithResource(String key, String resource) throws Exception {
                Thread.sleep(100);
            }

            @Override
            protected String getRequestKey() throws Exception {
                return "test-key";
            }
        };

        // borrow timeout >> doSomething() no timeout expected
        TestStats testStats = test.startTest(factory, config, 50, 200);
        assertEquals("We should see some timeoutRequests", true, testStats.timeoutRequests > 0);
    }

    public void testSocketPoolLimitSomeTimeout() throws Exception {
        // start a dummy server
        SocketServer server = new SocketServer(7666,
                                               50,
                                               50,
                                               1000,
                                               new SocketRequestHandlerFactory(null,
                                                                               null,
                                                                               null,
                                                                               null),
                                               "test");
        server.start();

        final ResourcePoolConfig config = new ResourcePoolConfig().setTimeout(50,
                                                                              TimeUnit.MILLISECONDS)
                                                                  .setMaxPoolSize(20);

        ResourceFactory<SocketDestination, SocketAndStreams> factory = ResourcePoolTestUtils.getSocketPoolFactory();
        final AbstractSocketPoolTest<SocketDestination, SocketAndStreams> test = new AbstractSocketPoolTest<SocketDestination, SocketAndStreams>() {

            @Override
            protected void doSomethingWithResource(SocketDestination key, SocketAndStreams resource)
                    throws Exception {
                Thread.sleep(100);
                int random = (int) (Math.random() * 10);
                if(random >= 5)
                    resource.getSocket().close();
            }

            @Override
            protected SocketDestination getRequestKey() throws Exception {
                return new SocketDestination("localhost", 7666, RequestFormatType.VOLDEMORT_V1);
            }

        };

        // borrow timeout >> doSomething() no timeout expected
        TestStats testStats = test.startTest(factory, config, 50, 200);
        assertEquals("We should see some timeoutRequests", true, testStats.timeoutRequests > 0);
        server.shutdown();
    }

    public void testNoTimeout() throws Exception {
        final ResourcePoolConfig config = new ResourcePoolConfig().setTimeout(100,
                                                                              TimeUnit.MILLISECONDS)
                                                                  .setMaxPoolSize(20);

        ResourceFactory<String, String> factory = ResourcePoolTestUtils.getBasicPoolFactory();
        final AbstractSocketPoolTest<String, String> test = new AbstractSocketPoolTest<String, String>() {

            @Override
            protected void doSomethingWithResource(String key, String resource) throws Exception {
                Thread.sleep(200);
            }

            @Override
            protected String getRequestKey() throws Exception {
                return "test-key";
            }
        };

        // threads same as pool size no timeout expected
        TestStats testStats = test.startTest(factory, config, 20, 200);
        assertEquals("We should see Zero timeoutRequests", 0, testStats.timeoutRequests);
    }
}
