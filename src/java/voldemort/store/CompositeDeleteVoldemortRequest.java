package voldemort.store;

import voldemort.common.VoldemortOpCode;
import voldemort.versioning.Version;

public class CompositeDeleteVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositeDeleteVoldemortRequest(K key, Version version, long timeout) {
        super(key, null, null, null, version, timeout, true, VoldemortOpCode.DELETE_OP_CODE);
    }
}
