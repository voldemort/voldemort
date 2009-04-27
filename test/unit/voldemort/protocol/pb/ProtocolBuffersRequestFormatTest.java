package voldemort.protocol.pb;

import voldemort.client.protocol.RequestFormatType;
import voldemort.protocol.AbstractRequestFormatTest;

public class ProtocolBuffersRequestFormatTest extends AbstractRequestFormatTest {

    public ProtocolBuffersRequestFormatTest() {
        super(RequestFormatType.PROTOCOL_BUFFERS);
    }

}
