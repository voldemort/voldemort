package voldemort.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class VoldemortIOUtilsTest {

    @Test
    public void testToString() throws IOException {
        // some input file which is > 30K;
        final int upperBound = 30000;
        InputStream is = getClass().getResourceAsStream("Xtranslcl.c.input");
        String str = IOUtils.toString(is);
        Assert.assertTrue(str.length() > 0);
        Assert.assertTrue(str.length() > upperBound);

        InputStream is2 = getClass().getResourceAsStream("Xtranslcl.c.input");
        String str2 = VoldemortIOUtils.toString(is2, upperBound);
        Assert.assertEquals(upperBound, str2.length());
    }

    @Test
    public void testToStringSmall() throws IOException {
        final int upperBound = 30000;
        InputStream is = getClass().getResourceAsStream("maze.c.input");
        String str2 = VoldemortIOUtils.toString(is, upperBound);
        Assert.assertTrue(str2.length() <= upperBound);
    }

}
