package voldemort.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PropsTest {

    private final static int TIMES = 1000;
    private final static Random randomGenerator = new Random();
    private final static int MAX_STRING_LENGTH = 20;

    private static String randomString() {
        final int length = randomGenerator.nextInt(MAX_STRING_LENGTH) + 1;
        final StringBuilder builder = new StringBuilder(length);

        for(int i = 0; i < length; i++) {
            final int random = randomGenerator.nextInt();
            builder.append(random);
        }

        return builder.toString();
    }

    private static Map<String, String> randomMap() {
        final int length = randomGenerator.nextInt();
        final Map<String, String> map = new HashMap<String, String>();

        for(int i = 0; i < length; i++) {
            final String randomKey = randomString();
            final String randomValue = randomString();

            map.put(randomKey, randomValue);
        }

        return map;
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testHashCode() {}

    @Test
    public void testProps() {}

    @Test
    public void testPropsFileArray() {}

    @Test
    public void testPropsMapOfStringStringArray() {}

    @Test
    public void testPropsPropertiesArray() {}

    @Test
    public void testLoadProperties() {}

    @Test
    public void testClear() {}

    @Test
    public void testContainsKey() {
        Props props = new Props();

        for(int i = 0; i < TIMES; i++) {
            final String randomKey = randomString();
            final String randomValue = randomString();

            props.put(randomKey, randomValue);

            assertThat(props.containsKey(randomKey), is(true));
            assertThat(props.containsKey(randomValue), is(false));
        }
    }

    @Test
    public void testContainsValue() {
        Props props = new Props();

        for(int i = 0; i < TIMES; i++) {
            final String randomKey = randomString();
            final String randomValue = randomString();

            props.put(randomKey, randomValue);

            assertThat(props.containsValue(randomValue), is(true));
            assertThat(props.containsValue(randomKey), is(false));
        }
    }

    @Test
    public void testEntrySet() {}

    @Test
    public void testGet() {}

    @Test
    public void testIsEmpty() {}

    @Test
    public void testKeySet() {}

    @Test
    public void testPutStringString() {}

    @Test
    public void testPutStringInteger() {}

    @Test
    public void testPutStringLong() {}

    @Test
    public void testPutStringDouble() {}

    @Test
    public void testWithStringString() {}

    @Test
    public void testWithStringInteger() {}

    @Test
    public void testWithStringDouble() {}

    @Test
    public void testWithStringLong() {}

    @Test
    public void testPutAll() {}

    @Test
    public void testRemove() {}

    @Test
    public void testSize() {}

    @Test
    public void testValues() {}

    @Test
    public void testGetStringStringString() {}

    @Test
    public void testGetStringString() {}

    @Test
    public void testGetBooleanStringBoolean() {}

    @Test
    public void testGetBooleanString() {}

    @Test
    public void testGetLongStringLong() {}

    @Test
    public void testGetLongString() {}

    @Test
    public void testGetIntStringInt() {}

    @Test
    public void testGetIntString() {}

    @Test
    public void testGetDoubleStringDouble() {}

    @Test
    public void testGetDoubleString() {}

    @Test
    public void testGetBytesStringLong() {}

    @Test
    public void testEqualsObject() {}

    @Test
    public void testToString() {}

    @Test
    public void testGetBytesString() {}

    @Test
    public void testGetListStringListOfString() {}

    @Test
    public void testGetListString() {}

}
