package voldemort.versioning;

import java.util.Collections;
import java.util.List;

/**
 * An inconsistency resolution strategy that always prefers the first of the two
 * objects.
 * 
 * @author jay
 * 
 */
public class ArbitraryInconsistencyResolver<T> implements InconsistencyResolver<T> {

    /**
     * Arbitrarily resolve the inconsistency by choosing the first object if
     * there is one.
     * 
     * @param values The list of objects
     * @return A single value, if one exists, taken from the input list.
     */
    public List<T> resolveConflicts(List<T> values) {
        if (values.size() > 1)
            return values;
        else
            return Collections.singletonList(values.get(0));
    }

}
