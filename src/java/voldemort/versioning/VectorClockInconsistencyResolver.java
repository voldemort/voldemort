package voldemort.versioning;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * An inconsistency resolver that uses the object VectorClocks when possible and
 * delegates to a user supplied semantic resolver when necessary.
 * 
 * @author jay
 * 
 */
public class VectorClockInconsistencyResolver<T> implements InconsistencyResolver<Versioned<T>> {

    /**
     * Combine the two objects by taking the object with the least version. If
     * both have concurrent versions use the user-supplied conflict resolver.
     */
    public List<Versioned<T>> resolveConflicts(List<Versioned<T>> items) {
        int size = items.size();
        if(size <= 1)
            return items;

        Collections.sort(items, new Versioned.HappenedBeforeComparator<T>());
        List<Versioned<T>> concurrent = Lists.newArrayList();
        Versioned<T> last = items.get(items.size() - 1);
        concurrent.add(last);
        for(int i = items.size() - 2; i >= 0; i--) {
            Versioned<T> curr = items.get(i);
            if(curr.getVersion().compare(last.getVersion()) == Occured.CONCURRENTLY)
                concurrent.add(curr);
            else
                break;
        }

        return concurrent;
    }

}
