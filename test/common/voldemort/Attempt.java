package voldemort;

/**
 */
public interface Attempt {
    public void checkCondition() throws Exception, AssertionError;
}
