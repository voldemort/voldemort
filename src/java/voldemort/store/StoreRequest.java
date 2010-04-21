package voldemort.store;

import voldemort.VoldemortException;
import voldemort.utils.ByteArray;

public interface StoreRequest<T> {

    public T request(Store<ByteArray, byte[]> store) throws VoldemortException;

}
