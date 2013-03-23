package voldemort.store;

import voldemort.common.VoldemortOpCode;

public class CompositeGetVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositeGetVoldemortRequest(K key, long timeout, boolean resolveConflicts) {
        super(key, null, null, null, null, timeout, resolveConflicts, VoldemortOpCode.GET_OP_CODE);
    }
}
