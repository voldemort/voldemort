package voldemort.store;

import voldemort.common.VoldemortOpCode;

public class CompositeGetAllVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositeGetAllVoldemortRequest(Iterable<K> keys, long timeout, boolean resolveConflicts) {
        super(null,
              null,
              keys,
              null,
              null,
              timeout,
              resolveConflicts,
              VoldemortOpCode.GET_ALL_OP_CODE);
    }

}
