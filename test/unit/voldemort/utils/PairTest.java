package voldemort.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.util.Random;

import org.junit.Test;

public class PairTest {

    private final static int TIMES = 10000;
    private final static Random randomGenerator = new Random();

    @Test
    public void testGetFirst_and_getSecond() {

        for(int i = 0; i < TIMES; i++) {
            final int first = randomGenerator.nextInt();
            final long second = randomGenerator.nextLong();
            final Pair<Integer, Long> pair = new Pair<Integer, Long>(first, second);

            assertThat(pair.getFirst(), is(first));
            assertThat(pair.getSecond(), is(second));
        }
    }

    @Test
    public void testEquals() {

        for(int i = 0; i < TIMES; i++) {
            final int first = randomGenerator.nextInt();
            final long second = randomGenerator.nextLong();
            final Pair<Integer, Long> pair = new Pair<Integer, Long>(first, second);

            assertThat(pair.equals(null), is(false));
            assertThat(pair.equals(randomGenerator), is(false));

            assertThat(pair.equals(new Pair<Integer, Long>(first + 1, second)), is(false));
            assertThat(pair.equals(new Pair<Integer, Long>(first, second + 1)), is(false));

            assertThat(pair.equals(new Pair<Integer, Long>(first, second)), is(true));
        }
    }

    @Test
    public void testApply() {

        for(int i = 0; i < TIMES; i++) {
            final int first = randomGenerator.nextInt();
            final long second = randomGenerator.nextLong();
            final Pair<Integer, Long> pair = new Pair<Integer, Long>(first, second);

            assertThat(pair.apply(first), is(second));
            assertThat((new Pair<Integer, Long>(null, second)).apply(null), is(second));

            assertNull(pair.apply(first + 1));

            assertNull((new Pair<Integer, Long>(first, null)).apply(first));
            assertNull((new Pair<Integer, Long>(first, null)).apply(null));
            assertNull((new Pair<Integer, Long>(null, second)).apply(first));
            assertNull((new Pair<Integer, Long>(null, null)).apply(null));
        }
    }

    @Test
    public void testCreate() {

        for(int i = 0; i < TIMES; i++) {
            final int first = randomGenerator.nextInt();
            final long second = randomGenerator.nextLong();

            assumeThat(Pair.create(first, second).equals(new Pair<Integer, Long>(first, second)),
                       is(true));
        }
    }

}
