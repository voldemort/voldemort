package voldemort;

import junit.framework.AssertionFailedError;

/**
 * @author afeinberg
 */
public interface Attempt {
    public void checkCondition() throws Exception, AssertionFailedError;
}
