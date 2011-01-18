package voldemort.store.readonly;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.utils.ByteUtils;

/**
 * A base test for testing search strategies for the read-only store
 * 
 * 
 */
@RunWith(Parameterized.class)
public class SearchStrategyTest {

    private SearchStrategy strategy;
    private int keyHashSize;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { new BinarySearchStrategy(), ByteUtils.SIZE_OF_BYTE },
                { new InterpolationSearchStrategy(), ByteUtils.SIZE_OF_BYTE },
                { new BinarySearchStrategy(), ByteUtils.SIZE_OF_INT },
                { new InterpolationSearchStrategy(), ByteUtils.SIZE_OF_INT } });
    }

    public SearchStrategyTest(SearchStrategy strategy, int keyHashSize) {
        this.strategy = strategy;
        this.keyHashSize = keyHashSize;
    }

    @Test
    public void findNothingInEmptyIndex() {
        ByteBuffer index = makeIndex(new byte[][] {}, new int[] {});
        assertKeysNotFound(index, key(-1, 2), key(9, 9));
    }

    @Test
    public void testSingleton() {
        byte[] theKey = key(1, 2);
        int theVal = 42;
        ByteBuffer index = makeIndex(new byte[][] { theKey }, new int[] { theVal });
        assertKeyFound(index, theKey, theVal);
        assertKeysNotFound(index, key(-1, 2), key(9, 9));
    }

    @Test
    public void keyOutOfRange() {
        byte[] startKey = key(0, 1);
        byte[] endKey = key(Long.MIN_VALUE, 0);
        ByteBuffer index = makeIndex(new byte[][] { startKey, endKey }, new int[] { 1, 2 });
        assertKeyFound(index, startKey, 1);
        assertKeyFound(index, endKey, 2);
        assertKeysNotFound(index, key(0, 0), key(Long.MIN_VALUE, 1));
    }

    @Test
    public void locationIsEndPoint() {
        byte[] startKey = key(0, 0);
        byte[] middleKey = key(5, 5);
        byte[] endKey = key(Long.MIN_VALUE, Long.MIN_VALUE);
        ByteBuffer index = makeIndex(new byte[][] { startKey, middleKey, endKey }, new int[] { 1,
                2, 3 });
        assertKeyFound(index, startKey, 1);
        assertKeyFound(index, middleKey, 2);
        assertKeyFound(index, endKey, 3);
        assertKeysNotFound(index, key(-1, 2), key(9, 9));
    }

    @Test
    public void testRandomValues() {
        Random rand = new Random(48534543);
        int size = 100;
        for(int iter = 0; iter < 10; iter++) {
            byte[][] keys = new byte[size][];
            int[] values = new int[size];
            for(int i = 0; i < size; i++) {
                byte[] key = new byte[keyHashSize];
                rand.nextBytes(key);
                values[i] = rand.nextInt(1000000);
                keys[i] = key;
            }
            ByteBuffer index = makeIndex(keys, values);
            // test finding keys
            for(int i = 0; i < size; i++)
                assertKeyFound(index, keys[i], values[i]);
            for(int i = 0; i < 10; i++) {
                byte[] key = new byte[keyHashSize];
                assertKeysNotFound(index, key);
            }

        }
    }

    public void print(byte[][] keys, int[] positions) {
        for(int i = 0; i < keys.length; i++) {
            System.out.println(ByteUtils.toHexString(keys[i]) + "\t" + positions[i]);
        }
    }

    public void print(ByteBuffer buffer, int size) {
        buffer.position(0);
        for(int i = 0; i < size; i++) {
            byte[] key = new byte[16];
            buffer.get(key);
            int position = buffer.getInt();
            System.out.println(ByteUtils.toHexString(key) + "\t" + position);
        }
    }

    public ByteBuffer makeIndex(byte[][] keys, int[] positions) {
        assertEquals(keys.length, positions.length);
        Map<byte[], Integer> m = new HashMap<byte[], Integer>();
        for(int i = 0; i < keys.length; i++)
            m.put(keys[i], positions[i]);
        byte[][] copy = keys.clone();
        Arrays.sort(copy, new Comparator<byte[]>() {

            public int compare(byte[] b1, byte[] b2) {
                return ByteUtils.compare(b1, b2);
            }
        });
        ByteBuffer buffer = ByteBuffer.allocate((keyHashSize + ByteUtils.SIZE_OF_INT) * copy.length);
        for(int i = 0; i < copy.length; i++) {
            buffer.put(copy[i]);
            buffer.putInt(m.get(copy[i]));
        }
        return buffer;
    }

    public byte[] key(long v1, long v2) {
        byte[] bytes = new byte[keyHashSize];
        ByteUtils.writeLong(bytes, v1, 0);
        ByteUtils.writeLong(bytes, v2, 8);
        return bytes;
    }

    public void assertKeyFound(ByteBuffer buffer, byte[] key, int expected) {
        int found = strategy.indexOf(buffer, key, buffer.limit());
        assertTrue("Failed to find correct key " + key, found != -1);
        assertEquals(expected, found);
    }

    public void assertKeysNotFound(ByteBuffer buffer, byte[]... keys) {
        for(byte[] key: keys) {
            assertTrue("Failed to find key " + key,
                       strategy.indexOf(buffer, key, buffer.limit()) == -1);
        }
    }

}
