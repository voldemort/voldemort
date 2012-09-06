package voldemort.client.scheduler;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import voldemort.client.SystemStoreRepository;
import voldemort.utils.MetadataVersionStoreUtils;

/*
 * The AsyncMetadataVersionManager is used to track the Metadata version on the
 * cluster and if necessary Re-bootstrap the client.
 * 
 * During initialization, it will retrieve the current version of the
 * cluster.xml and then periodically check whether this has been updated. During
 * init if the initial version turns out to be null, it means that no change has
 * been done to that store since it was created. In this case, we assume version
 * '0'.
 */

public class AsyncMetadataVersionManager implements Runnable {

    public static final String CLUSTER_VERSION_KEY = "cluster.xml";
    public static final String VERSIONS_METADATA_STORE = "metadata-versions";

    private final Logger logger = Logger.getLogger(this.getClass());
    private Long currentClusterVersion;
    private final Callable<Void> storeClientThunk;
    private final SystemStoreRepository sysRepository;

    // Random delta generator
    private final int DELTA_MAX = 2000;
    private final Random randomGenerator = new Random(System.currentTimeMillis());

    public boolean isActive = false;

    public AsyncMetadataVersionManager(SystemStoreRepository sysRepository,
                                       Callable<Void> storeClientThunk) {
        this.sysRepository = sysRepository;

        // Get the properties object from the system store (containing versions)
        Properties versionProps = MetadataVersionStoreUtils.getProperties(this.sysRepository.getMetadataVersionStore());

        try {
            this.currentClusterVersion = getCurrentVersion(CLUSTER_VERSION_KEY, versionProps);
        } catch(Exception e) {
            logger.error("Exception while getting currentClusterVersion : " + e);
        }

        // If the received version is null, assume version 0
        if(currentClusterVersion == null) {
            currentClusterVersion = new Long(0);
        }
        logger.debug("Initial cluster.xml version = " + this.currentClusterVersion);

        this.storeClientThunk = storeClientThunk;
        this.isActive = true;
    }

    public Long getCurrentVersion(String versionKey, Properties versionProps) {
        Long versionValue = null;

        if(versionProps.getProperty(versionKey) != null) {
            versionValue = Long.parseLong(versionProps.getProperty(versionKey));
        }

        logger.debug("*********** For key : " + versionKey + " received value = " + versionValue);
        return versionValue;
    }

    /*
     * This method checks for any update in the version for 'versionKey'. If
     * there is any change, it returns the new version. Otherwise it will return
     * a null.
     */
    public Long fetchNewVersion(String versionKey, Long curVersion, Properties versionProps) {
        try {
            Long newVersion = getCurrentVersion(versionKey, versionProps);

            // If version obtained is null, the store is untouched. Continue
            if(newVersion != null) {
                logger.debug("MetadataVersion check => Obtained " + versionKey + " version : "
                             + newVersion);

                if(!newVersion.equals(curVersion)) {
                    return newVersion;
                }
            } else {
                logger.debug("Metadata unchanged after creation ...");
            }
        }

        // Swallow all exceptions here (we dont want to fail the client).
        catch(Exception e) {
            logger.info("Could not retrieve Metadata Version. Exception : " + e);
        }

        return null;
    }

    public void run() {

        // Get the properties object from the system store (containing versions)
        Properties versionProps = MetadataVersionStoreUtils.getProperties(this.sysRepository.getMetadataVersionStore());
        Long newClusterVersion = fetchNewVersion(CLUSTER_VERSION_KEY,
                                                 currentClusterVersion,
                                                 versionProps);

        // If nothing has been updated, continue
        if(newClusterVersion != null) {

            logger.info("Metadata version mismatch detected.");

            // Determine a random delta delay between 0 to DELTA_MAX to sleep
            int delta = randomGenerator.nextInt(DELTA_MAX);

            try {
                logger.info("Sleeping for delta : " + delta + " (ms) before re-bootstrapping.");
                Thread.sleep(delta);
            } catch(InterruptedException e) {
                // do nothing, continue.
            }

            /*
             * Do another check for mismatch here since the versions might have
             * been updated while we were sleeping
             */
            if(!newClusterVersion.equals(currentClusterVersion)) {

                try {
                    logger.info("Updating cluster version");
                    currentClusterVersion = newClusterVersion;

                    this.storeClientThunk.call();
                } catch(Exception e) {
                    e.printStackTrace();
                    logger.info(e.getMessage());
                }
            }
        }

    }

    public Long getClusterMetadataVersion() {
        return this.currentClusterVersion;
    }

    // Fetch the latest versions for cluster and store
    public void updateMetadataVersions() {
        Properties versionProps = MetadataVersionStoreUtils.getProperties(this.sysRepository.getMetadataVersionStore());
        this.currentClusterVersion = fetchNewVersion(CLUSTER_VERSION_KEY, null, versionProps);
    }
}
