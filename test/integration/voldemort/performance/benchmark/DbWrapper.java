package voldemort.performance.benchmark;

/**
 * @author nnarkhed
 * 
 */
public interface DbWrapper {

    public int read(Object key, Object expectedValue);

    public int read(Object key, Object expectedValue, Object transforms);

    public int mixed(final Object key, final Object newValue);

    public int mixed(final Object key, final Object newValue, final Object transforms);

    public int write(final Object key, final Object value);

    public int write(final Object key, final Object value, final Object transforms);

    public int delete(Object key);
}
