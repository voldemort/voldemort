package voldemort.store.readonly;

import java.io.File;
import java.io.IOException;

import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.metadata.MetadataStore;


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

    /**
     * This code path is only used by tests and by the old unused
     * {@link voldemort.server.http.gui.ReadOnlyStoreManagementServlet}.
     *
     * Do not use anymore, as it may go away in the future.
     *
     * @param source
     * @param dest
     * @return
     * @throws IOException
     * @throws Exception
     */
    @Deprecated
    public File fetch(String source, String dest) throws IOException, Exception;

    public File fetch(String source,
                      String dest,
                      AsyncOperationStatus status,
                      String storeName,
                      long pushVersion,
                      MetadataStore metadataStore) throws IOException, Exception;
}
