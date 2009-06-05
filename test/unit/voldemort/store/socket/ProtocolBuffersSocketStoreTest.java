package voldemort.store.socket;

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

}
