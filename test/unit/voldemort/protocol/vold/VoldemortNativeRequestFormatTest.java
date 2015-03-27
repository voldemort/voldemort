package voldemort.protocol.vold;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.client.protocol.RequestFormatType;
import voldemort.protocol.AbstractRequestFormatTest;

@RunWith(Parameterized.class)
public class VoldemortNativeRequestFormatTest extends AbstractRequestFormatTest {

    public VoldemortNativeRequestFormatTest(RequestFormatType type) {
        super(type);
    }
    
    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { RequestFormatType.VOLDEMORT_V1 },
                { RequestFormatType.VOLDEMORT_V2 }, { RequestFormatType.VOLDEMORT_V3 } });
    }


}
