package voldemort.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.util.Random;

import org.junit.Test;

public class PropsTest {

    private final static Random randomGenerator = new Random();
    private final static int MAX_STRING_LENGTH = 10;
    private final static int MAX_PROPS_SIZE = 10;
    private final static int TIMES = 1000;

    /**
     * 
     * @return a {@code String} of length between {@literal 1} and
     *         {@code MAX_STRING_LENGTH} inclusive
     */
    private static String generateString() {
        final int length = randomGenerator.nextInt(MAX_STRING_LENGTH) + 1;
        final StringBuilder builder = new StringBuilder(length);

        for(int i = 0; i < length; i++) {
            final int random = randomGenerator.nextInt();
            builder.append(random);
        }

        return builder.toString();
    }

    /**
     * 
     * @return a {@code Props} of size between {@literal 1} and
     *         {@code MAX_PROPS_LENGTH} inclusive
     */
    private static Props generateProps() {
        final int length = randomGenerator.nextInt(MAX_PROPS_SIZE) + 1;
        final Props props = new Props();

        for(int i = 0; i < length; i++) {
            props.put(generateString(), generateString());
        }

        return props;
    }

    @Test
    public void testClear_Remove_With_Size() {
        for(int i = 0; i < TIMES; i++) {
            final Props props = generateProps();
            final String randomKey = generateString();
            final String randomValue = generateString();
            final int size = props.size();
            final String key = props.keySet().iterator().next();

            assertThat(props.with(randomKey, randomValue).get(randomKey), is(randomValue));

            props.remove(key);

            assumeThat(props.size(), is(size - 1));

            assumeThat(props.size() > 0, is(true));
            assumeThat(props.isEmpty(), is(false));

            props.clear();

            assumeThat(props.size() == 0, is(true));
            assumeThat(props.isEmpty(), is(true));
        }
    }

    @Test
    public void testContainsKey_ContainsValue_Get_Size() {
        Props props = new Props();

        for(int i = 0; i < TIMES; i++) {
            final String randomKey = generateString();
            final String randomValue = generateString();
            final int size = props.size();

            props.put(randomKey, randomValue);

            assertThat(props.containsKey(randomKey), is(true));
            assertThat(props.containsKey(randomValue), is(false));

            assertThat(props.containsValue(randomValue), is(true));
            assertThat(props.containsValue(randomKey), is(false));

            assertThat(props.get(randomKey), is(randomValue));
            assertNull(props.get(randomValue));

            assertThat(props.size(), is(size + 1));
        }
    }

}
