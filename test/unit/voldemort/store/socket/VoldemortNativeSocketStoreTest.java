package voldemort.store.socket;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

    public VoldemortNativeSocketStoreTest(RequestFormatType type, boolean useNio) {
        super(RequestFormatType.VOLDEMORT_V1, useNio);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        RequestFormatType[] types = new RequestFormatType[] {
                RequestFormatType.VOLDEMORT_V1,
                RequestFormatType.VOLDEMORT_V2,
                RequestFormatType.VOLDEMORT_V3
 };
        List<Object[]> options = new ArrayList<Object[]>();
        boolean[] nioOptions = { true, false };
        for(RequestFormatType type: types){
            for(boolean nio: nioOptions) {
                options.add(new Object[] { type, nio });
            }
        }
        return options;
    }

}
