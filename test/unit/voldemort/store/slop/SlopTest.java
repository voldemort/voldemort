package voldemort.store.slop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.utils.ByteUtils;

public class SlopTest {

    @Test
    public void testMakeKey() {
        // Basic test for unique key generation
        byte[] key = TestUtils.randomBytes(10);
        byte[] value1 = TestUtils.randomBytes(100), value2 = TestUtils.randomBytes(200);

        Date date = new Date();
        Slop s1 = new Slop("store", Slop.Operation.PUT, key, value1, 0, date);
        Slop s2 = new Slop("store", Slop.Operation.PUT, key, value2, 0, date);
        Slop s3 = new Slop("store", Slop.Operation.PUT, key, value1, 1, date);
        Slop s4 = new Slop("store2", Slop.Operation.DELETE, key, value1, 1, date);
        Slop s5 = new Slop("store2", Slop.Operation.PUT, key, value1, 0, date);

        assertEquals(0, ByteUtils.compare(s1.makeKey().get(), s2.makeKey().get()));
        assertTrue(0 != ByteUtils.compare(s1.makeKey().get(), s3.makeKey().get()));
        assertTrue(0 != ByteUtils.compare(s3.makeKey().get(), s4.makeKey().get()));
        assertTrue(0 != ByteUtils.compare(s4.makeKey().get(), s5.makeKey().get()));
    }
}
