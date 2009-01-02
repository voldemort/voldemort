package voldemort.store.readonly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import voldemort.serialization.StringSerializer;

import com.google.common.collect.Lists;

public class ExternalSorterTest extends TestCase {

    private List<String> abcs;

    public void setUp() {
        abcs = new ArrayList<String>();
        String abcStr = "QERTYUIOLKJHGFDSAMNBVCXZqwertyuiopasghjklzxcvbnm";
        for(int i = 0; i < abcStr.length(); i++)
            abcs.add(abcStr.substring(i, i + 1));
    }

    public void testSorting() {
        ExternalSorter<String> sorter = new ExternalSorter<String>(new StringSerializer(), 10);
        List<String> sorted = Lists.newArrayList(sorter.sorted(abcs.iterator()));
        List<String> expected = new ArrayList<String>(abcs);
        Collections.sort(expected);
        assertEquals(expected, sorted);
    }

}
