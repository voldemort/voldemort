package voldemort.server.socket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.VoldemortTestConstants;
import voldemort.server.AbstractSocketService;

public class SocketServerTest {

    private AbstractSocketService socketService;
    private int port;

    @Before
    public void setUp() {
        port = ServerTestUtils.findFreePort();
        socketService = createSocketService();
        socketService.start();
    }

    @After
    public void tearDown() {
        socketService.stop();
    }

    private AbstractSocketService createSocketService() {
        return ServerTestUtils.getSocketService(false,
                                                VoldemortTestConstants.getOneNodeClusterXml(),
                                                VoldemortTestConstants.getSimpleStoreDefinitionsXml(),
                                                "test",
                                                port);
    }

    @Test(expected = VoldemortException.class)
    public void testStartOnUsedPortThrowsVoldemortException() {
        createSocketService().start();
    }
}
