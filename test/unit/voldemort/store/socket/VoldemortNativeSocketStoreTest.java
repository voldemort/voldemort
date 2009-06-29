package voldemort.store.socket;

import voldemort.client.protocol.RequestFormatType;

/**
 * Voldemort native socket store tests
 * 
 * @author jay
 * 
 */
public class VoldemortNativeSocketStoreTest extends AbstractSocketStoreTest {

    public VoldemortNativeSocketStoreTest() {
        super(RequestFormatType.VOLDEMORT_V1);
    }

}
