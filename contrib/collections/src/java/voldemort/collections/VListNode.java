package voldemort.collections;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import voldemort.utils.Identified;

/**
 * Immutable wrapper around an object that adds a Integer id, nextId,
 * previousId, and stable. Used for implementing larger list-based structures in
 * voldemort.
 */
public class VListNode<T> extends Identified<T, Integer> {

    /*
     * specifies if this node is participating in a structural change (i.e.,
     * add/remove)
     */
    public static final String STABLE = "stable";

    private static final String NEXT_ID = "nextId";
    private static final String PREVIOUS_ID = "previousId";
    private static final String ID = "id";
    private static final String VALUE = "value";

    private final Map<String, Object> _map;

    public VListNode(T object, Integer id, Integer prev, Integer next, Boolean stable) {
        super(object, id);

        Map<String, Object> tempMap = new HashMap<String, Object>();
        tempMap.put(ID, id);
        tempMap.put(VALUE, object);
        tempMap.put(NEXT_ID, next);
        tempMap.put(PREVIOUS_ID, prev);
        tempMap.put(STABLE, stable);
        _map = Collections.unmodifiableMap(tempMap);
    }

    public static <T> VListNode<T> valueOf(Map<String, Object> map) {
        @SuppressWarnings("unchecked")
        T value = (T) map.get(VALUE);
        return new VListNode<T>(value,
                                (Integer) map.get(ID),
                                (Integer) map.get(PREVIOUS_ID),
                                (Integer) map.get(NEXT_ID),
                                (Boolean) map.get(STABLE));
    }

    public Integer getNextId() {
        return (Integer) _map.get(NEXT_ID);
    }

    public Integer getPreviousId() {
        return (Integer) _map.get(PREVIOUS_ID);
    }

    public Map<String, Object> mapValue() {
        return _map;
    }

    public Boolean isStable() {
        return (Boolean) _map.get(STABLE);
    }

    public String toString() {
        return _map.toString();
    }
}
