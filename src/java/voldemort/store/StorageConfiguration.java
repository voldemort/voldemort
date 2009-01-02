package voldemort.store;

/**
 * An abstraction that represents the shared resources of a persistence engine.
 * This could include file handles, db connection pools, caches, etc.
 * 
 * For example for BDB it holds the various environments,
 * for jdbc it holds a connection pool reference
 * 
 * This should be called StorageEngineFactory but that would leave us with the indignity of
 * having a StorageEngineFactoryFactory to handle the mapping of store type => factory.  
 * And we can't have that.
 * 
 * @author jay
 *
 */
public interface StorageConfiguration {
	
	/**
	 * Get an initialized storage implementation
	 * @param name The name of the storage
	 * @return The storage engine
	 */
	public StorageEngine<byte[], byte[]> getStore(String name);
	
	/**
	 * Get the type of stores returned by this configuration
	 */
	public StorageEngineType getType();

	/**
	 * Close the storage configuration
	 */
	public void close();
}
