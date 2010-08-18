package voldemort.store.readonly;

import java.io.File;
import java.io.IOException;

import voldemort.server.protocol.admin.AsyncOperationStatus;

/**
 * An interface to fetch data for readonly store. The fetch could be via rsync
 * or hdfs. If the store is already on the local filesystem then no fetcher is
 * needed.
 * 
 * All implementations must provide a public constructor that takes
 * VoldemortConfig as a parameter.
 * 
 * 
 */
public interface FileFetcher {

    public File fetch(String source, String dest) throws IOException;

    public void setAsyncOperationStatus(AsyncOperationStatus status);
}
