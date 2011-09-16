package voldemort.collections;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import voldemort.utils.Identified;

/**
 * @author jko
 * 
 *         Pointer to a VListNode.
 */
public class VListKey<T> extends Identified<T, Integer> {

    private final Map<String, Object> _map;

    public VListKey(T object, Integer id) {
        super(object, id);

        Map<String, Object> tempMap = new HashMap<String, Object>();
        tempMap.put("id", id);
        tempMap.put("value", object);
        _map = Collections.unmodifiableMap(tempMap);
    }

    public Map<String, Object> mapValue() {
        return _map;
    }

    public String toString() {
        return _map.toString();
    }
}
