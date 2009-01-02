package voldemort.versioning;

import java.util.Collections;
import java.util.List;

/**
 * Resolve inconsistencies based on timestamp in the vector clock
 * 
 * @author jay
 * 
 * @param <T> The type f the versioned object
 */
public class TimeBasedInconsistencyResolver<T> implements InconsistencyResolver<Versioned<T>> {

    public List<Versioned<T>> resolveConflicts(List<Versioned<T>> items) {
        if(items.size() <= 1) {
            return items;
        } else {
            Versioned<T> max = items.get(0);
            long maxTime = ((VectorClock) items.get(0).getVersion()).getTimestamp();
            for(Versioned<T> versioned : items) {
                VectorClock clock = (VectorClock) versioned.getVersion();
                if(clock.getTimestamp() > maxTime) {
                    max = versioned;
                    maxTime = ((VectorClock) versioned.getVersion()).getTimestamp();
                }
            }
            return Collections.singletonList(max);
        }
    }

}
