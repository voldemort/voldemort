package voldemort.server.socket;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.VoldemortTestConstants;
import voldemort.server.AbstractSocketService;

@RunWith(Parameterized.class)
public class SocketServerTest {

    private AbstractSocketService socketService;
    private int port;

    private final boolean useNio;

    public SocketServerTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

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
        return ServerTestUtils.getSocketService(useNio,
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
