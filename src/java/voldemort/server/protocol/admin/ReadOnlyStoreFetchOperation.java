package voldemort.server.protocol.admin;

import java.io.File;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.utils.Utils;


public class ReadOnlyStoreFetchOperation extends AsyncOperation {

    enum State {
        WAITING,
        RUNNING,
        STOPPING,
        COMPLETED;
    }

    private final static Logger logger = Logger.getLogger(ReadOnlyStoreFetchOperation.class);

    private final FileFetcher fileFetcher;
    private final String fetchUrl;
    private final MetadataStore metadataStore;
    private final String storeName;
    private final long pushVersion;
    private final ReadOnlyStorageEngine store;
    private final long startTime = System.currentTimeMillis();
    private final Long diskQuotaSizeInKB;

    private State currentState;

    public ReadOnlyStoreFetchOperation(int id,
                                       MetadataStore metadataStore,
                                       ReadOnlyStorageEngine store,
                                       FileFetcher fileFetcher,
                                       String storeName,
                                       String fetchUrl,
                                       long pushVersion,
                                       Long diskQuotaSizeInKB) {
        super(id, "Fetch store '" + storeName + "' v" + pushVersion);
        this.metadataStore = metadataStore;
        this.store = store;
        this.fileFetcher = fileFetcher;
        this.storeName = storeName;
        this.fetchUrl = fetchUrl;
        this.pushVersion = pushVersion;
        this.currentState = State.WAITING;
        this.diskQuotaSizeInKB = diskQuotaSizeInKB;

        updateStatus("Waiting in Queue");
    }

    private String fetchDirPath = null;

    @Override
    public void markComplete() {
        if(fetchDirPath != null)
            status.setStatus(fetchDirPath);
        status.setComplete(true);
    }

    @Override
    public void operate() {
        this.currentState = State.RUNNING;

        File fetchDir = null;

        if(fileFetcher == null) {

            logger.warn("File fetcher class has not instantiated correctly. Assuming local file");

            if(!Utils.isReadableDir(fetchUrl)) {
                throw new VoldemortException("Fetch url " + fetchUrl + " is not readable");
            }

            fetchDir = new File(store.getStoreDirPath(), "version-" + Long.toString(pushVersion));

            if(fetchDir.exists())
                throw new VoldemortException("Version directory " + fetchDir.getAbsolutePath()
                                             + " already exists");

            Utils.move(new File(fetchUrl), fetchDir);

        } else {

            logger.info("Started executing fetch of " + fetchUrl + " for RO store '" + storeName
                        + "' version " + pushVersion);
            updateStatus("0 MB copied at 0 MB/sec - 0 % complete");

            try {

                String destinationDir = store.getStoreDirPath() + File.separator + "version-"
                                        + Long.toString(pushVersion);
                fetchDir = fileFetcher.fetch(fetchUrl,
                                             destinationDir,
                                             status,
                                             storeName,
                                             pushVersion,
                                             metadataStore,
                                             diskQuotaSizeInKB);
                if(fetchDir == null) {
                    String errorMessage = "File fetcher failed for " + fetchUrl + " and store '"
                                          + storeName
                                          + "' due to incorrect input path/checksum error";
                    updateStatus(errorMessage);
                    logger.error(errorMessage);
                    throw new VoldemortException(errorMessage);
                } else {
                    String message = "Successfully executed fetch of " + fetchUrl
                                     + " for RO store '" + storeName + "'";
                    updateStatus(message);
                    logger.info(message);
                }
            } catch(VoldemortException ve) {
                String errorMessage = "File fetcher failed for " + fetchUrl + " and store '"
                                      + storeName + "' Reason: \n" + ve.getMessage();
                updateStatus(errorMessage);
                logger.error(errorMessage, ve);
                throw ve;
            } catch(Exception e) {
                throw new VoldemortException("Exception in Fetcher = " + e.getMessage(), e);
            }

        }
        fetchDirPath = fetchDir.getAbsolutePath();
    }

    @Override
    public void stop() {
        this.currentState = State.STOPPING;
        status.setException(new AsyncOperationStoppedException("Fetcher interrupted"));
    }

    @Override
    public long getWaitTimeMs() {
        if(currentState == State.WAITING) {
            return System.currentTimeMillis() - startTime;
        }
        return 0L;
    }

    @Override
    public boolean isWaiting() {
        return currentState == State.WAITING;
    }
}
