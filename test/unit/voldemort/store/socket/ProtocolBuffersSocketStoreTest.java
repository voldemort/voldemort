package voldemort.store.socket;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.client.protocol.RequestFormatType;

/**
 * Protocol buffers socket store tests
 * 
 * 
 */

@RunWith(Parameterized.class)
public class ProtocolBuffersSocketStoreTest extends AbstractSocketStoreTest {

    public ProtocolBuffersSocketStoreTest(boolean useNio) {
        super(RequestFormatType.PROTOCOL_BUFFERS, useNio);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

}
