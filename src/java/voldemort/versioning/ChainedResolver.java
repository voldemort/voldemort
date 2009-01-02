package voldemort.versioning;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

/**
 * Apply the given inconsistency resolvers in order until there are 1 or fewer
 * items left.
 * 
 * @author jay
 * 
 */
public class ChainedResolver<T> implements InconsistencyResolver<T> {

    private List<InconsistencyResolver<T>> resolvers;

    public ChainedResolver(InconsistencyResolver<T>... resolvers) {
        this.resolvers = new ArrayList<InconsistencyResolver<T>>(resolvers.length);
        for (InconsistencyResolver<T> resolver : resolvers)
            this.resolvers.add(Objects.nonNull(resolver));
    }

    public List<T> resolveConflicts(List<T> items) {
        for (InconsistencyResolver<T> resolver : resolvers) {
            if (items.size() <= 1)
                return items;
            else
                items = resolver.resolveConflicts(items);
        }

        return items;
    }

}
