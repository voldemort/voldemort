package voldemort.store.socket;

import junit.framework.Test;
import voldemort.TestUtils;
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

    public static Test suite() {
        return TestUtils.createSocketServiceTestCaseSuite(VoldemortNativeSocketStoreTest.class);
    }

}
