package voldemort.server.protocol;

import voldemort.VoldemortException;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

/**
 * A base class with a few helper fields for writing a
 * {@link voldemort.server.protocol.RequestHandler}
 * 
 * 
 */
public abstract class AbstractRequestHandler implements RequestHandler {

    private final ErrorCodeMapper errorMapper;
    private final StoreRepository storeRepository;

    protected AbstractRequestHandler(ErrorCodeMapper errorMapper, StoreRepository repository) {
        this.errorMapper = errorMapper;
        this.storeRepository = repository;
    }

    protected ErrorCodeMapper getErrorMapper() {
        return errorMapper;
    }

    protected StoreRepository getStoreRepository() {
        return storeRepository;
    }

    protected Store<ByteArray, byte[], byte[]> getStore(String name, RequestRoutingType type) {

        switch(type) {
            case ROUTED:
                return storeRepository.getRoutedStore(name);
            case NORMAL:
                return storeRepository.getLocalStore(name);
            case IGNORE_CHECKS:
                return storeRepository.getStorageEngine(name);
        }

        throw new VoldemortException("Unhandled RoutingType found:" + type);
    }
}
