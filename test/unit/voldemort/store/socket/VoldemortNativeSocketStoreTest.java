package voldemort.store.socket;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.client.protocol.RequestFormatType;

/**
 * Voldemort native socket store tests
 * 
 * 
 */

@RunWith(Parameterized.class)
public class VoldemortNativeSocketStoreTest extends AbstractSocketStoreTest {

    public VoldemortNativeSocketStoreTest(boolean useNio) {
        super(RequestFormatType.VOLDEMORT_V1, useNio);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

}
