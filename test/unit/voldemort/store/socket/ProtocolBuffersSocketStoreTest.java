package voldemort.store.socket;

import junit.framework.Test;
import voldemort.TestUtils;
import voldemort.client.protocol.RequestFormatType;

/**
 * Protocol buffers socket store tests
 * 
 * @author jay
 * 
 */
public class ProtocolBuffersSocketStoreTest extends AbstractSocketStoreTest {

    public ProtocolBuffersSocketStoreTest() {
        super(RequestFormatType.PROTOCOL_BUFFERS);
    }

    public static Test suite() {
        return TestUtils.createSocketServiceTestCaseSuite(ProtocolBuffersSocketStoreTest.class);
    }

}
