package voldemort.server.protocol;

import voldemort.server.StoreRepository;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

/**
 * A base class with a few helper fields for writing a
 * {@link voldemort.server.protocol.RequestHandler}
 * 
 * @author jay
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

    protected StoreRepository getStoreRepository()
    {
        return storeRepository;
    }
    
    protected Store<ByteArray, byte[]> getStore(String name, boolean isRouted) {
        if(isRouted)
            return storeRepository.getRoutedStore(name);
        else
            return storeRepository.getLocalStore(name);
    }
}
