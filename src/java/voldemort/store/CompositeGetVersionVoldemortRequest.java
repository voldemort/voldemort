package voldemort.store;

import voldemort.common.VoldemortOpCode;
import voldemort.server.RequestRoutingType;

/**
 * A class that defines a composite get version request containing a reference
 * to the key, a flag to indicate if the conflicts should be resolved, the
 * timeout, routing type and origin time
 * 
 */
public class CompositeGetVersionVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositeGetVersionVoldemortRequest(K key, long timeoutInMs, boolean resolveConflicts) {
        super(key,
              null,
              null,
              null,
              null,
              timeoutInMs,
              resolveConflicts,
              VoldemortOpCode.GET_VERSION_OP_CODE);
    }

    // RestServerErrorHandler uses this constructor
    public CompositeGetVersionVoldemortRequest(K key,
                                               long timeoutInMs,
                                               long originTimeInMs,
                                               RequestRoutingType routingType) {
        super(key,
              null,
              null,
              null,
              null,
              timeoutInMs,
              false,
              VoldemortOpCode.GET_VERSION_OP_CODE,
              originTimeInMs,
              routingType);

    }
}
