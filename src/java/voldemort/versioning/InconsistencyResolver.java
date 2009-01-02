package voldemort.versioning;

import java.util.List;

/**
 * A method for resolving inconsistent object values into a single value
 * 
 * @author jay
 * 
 */
public interface InconsistencyResolver<T> {

    /**
     * Take two different versions of an object and combine them into a single
     * version of the object Implementations must maintain the contract that 1.
     * resolveConflict([null, null]) = null 2. if t != null, then
     * resolveConflict([null, t]) = resolveConflict([t, null]) = t
     * 
     * @param items The items to be resolved
     * @return The united object
     */
    public List<T> resolveConflicts(List<T> items);

}
