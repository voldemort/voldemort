package voldemort.utils;

/**
 * @author afeinber
 *
 * Pseudo random number generator. Re-implemented to produce strictly repeatable
 * experiments (want to start with a specific seed and generate a known sequence of
 * random long numbers).
 */
public class PseudoRandom {
    private long seed;

    // Good default parameters for the random number generator.
    private final long A = 16807;
    private final long M = 2147483647;
    private final long Q = 127773;
    private final long R = 2836;

    /**
     * Default constructor. Uses a recommended default seed
     */
    public PseudoRandom() {
        this.seed = 26302577; // A good default seed
    }

    /**
     * Create a pseudo random number generator given a specific seed
     *
     * @param seed Seed to use
     */
    public PseudoRandom(long seed) {
        this.seed = seed;
    }

    /**
     * Create the next random number, given the current seed
     *
     * @param x Current seed
     * @return Next random number
     */
    private long random(long x) {
        long x_ = A * (x % Q) - R * (x / Q);
        return x_ > 0 ? x_ : x_ + M;
    }

    /**
     * Random numbers will be returned in a consistent
     * sequence after each construction of PseudoRandom given the same seed, allowing
     * for repeatable experiments. This method is synchronized for thread safety
     *
     * @return A new random number.
     */
    public synchronized long random() {
        this.seed = random(seed);
        return seed;
    }
}
