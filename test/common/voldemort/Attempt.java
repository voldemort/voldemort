package voldemort;

/**
 * @author afeinberg
 */
public interface Attempt {
    public void checkCondition() throws Exception, AssertionError;
}
