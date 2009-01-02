package voldemort.versioning;

import java.util.List;

/**
 * An inconsistency resolver that does not attempt to resolve inconsistencies,
 * but instead just throws an exception if one should occur.
 * 
 * @author jay
 */
public class FailingInconsistencyResolver<T> implements InconsistencyResolver<T> {

    public List<T> resolveConflicts(List<T> items) {
        if (items.size() > 1)
            throw new InconsistentDataException("Conflict resolution failed.", items);
        else
            return items;
    }

}
