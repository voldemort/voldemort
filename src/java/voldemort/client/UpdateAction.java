package voldemort.client;

/**
 * An update action such as a read-modify-store cycle
 * 
 * @author jay
 * 
 */
public abstract class UpdateAction<K, V> {

    /**
     * Apply the update sequence
     * 
     * @param storeClient The store client to use
     */
    public abstract void update(StoreClient<K, V> storeClient);

    /**
     * A hook for the user to override with any rollback actions they want
     * performed when the update fails (say due to an exception or due to too
     * many ObsoleteVersionExceptions).
     */
    public void rollback() {}

}
