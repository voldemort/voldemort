package voldemort.versioning;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A strategy based on merging the objects in the list
 * 
 * @author jay
 * 
 */
public class MergingInconsitencyResolver<T> implements InconsistencyResolver<Versioned<T>> {

    private final ObjectMerger<T> merger;

    public MergingInconsitencyResolver(ObjectMerger<T> merger) {
        this.merger = merger;
    }

    public List<Versioned<T>> resolveConflicts(List<Versioned<T>> items) {
        if (items.size() <= 1) {
            return items;
        } else {
            Iterator<Versioned<T>> iter = items.iterator();
            Versioned<T> current = iter.next();
            T merged = current.getValue();
            VectorClock clock = (VectorClock) current.getVersion();
            while (iter.hasNext()) {
                Versioned<T> versioned = iter.next();
                merged = merger.merge(merged, versioned.getValue());
                clock = clock.merge((VectorClock) versioned.getVersion());
            }
            return Collections.singletonList(new Versioned<T>(merged, clock));
        }
    }

}
