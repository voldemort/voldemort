package voldemort.protocol.vold;

import voldemort.client.protocol.RequestFormatType;
import voldemort.protocol.AbstractRequestFormatTest;

public class VoldemortNativeRequestFormatTest extends AbstractRequestFormatTest {

    public VoldemortNativeRequestFormatTest() {
        super(RequestFormatType.VOLDEMORT);
    }

}
