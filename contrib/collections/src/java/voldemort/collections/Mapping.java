package voldemort.collections;

import java.util.Map;

/**
 * Interface that allows an object to expose a Map view of itself.
 * 
 * @author jko
 *
 * @param <K> Map key type
 * @param <E> Map value type
 */
public interface Mapping<K, E>
{ 
   Map<K, E> asMap();
}
