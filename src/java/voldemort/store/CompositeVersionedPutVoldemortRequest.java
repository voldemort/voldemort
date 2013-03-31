package voldemort.store;

import voldemort.common.VoldemortOpCode;
import voldemort.versioning.Versioned;

public class CompositeVersionedPutVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositeVersionedPutVoldemortRequest(K key, Versioned<V> value, long timeout) {
        super(key, null, null, value, null, timeout, true, VoldemortOpCode.PUT_OP_CODE);
    }

}
