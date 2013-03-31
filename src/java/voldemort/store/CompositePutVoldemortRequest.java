package voldemort.store;

import voldemort.common.VoldemortOpCode;

public class CompositePutVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositePutVoldemortRequest(K key, V rawValue, long timeout) {
        super(key, rawValue, null, null, null, timeout, true, VoldemortOpCode.PUT_OP_CODE);
    }
}
