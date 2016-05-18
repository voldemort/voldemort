package voldemort.store.readonly;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import voldemort.server.VoldemortConfig;
import voldemort.store.PersistenceFailureException;
import voldemort.store.readonly.swapper.FailedFetchLock;
import voldemort.utils.Props;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Map;

/**
 * This class helps manage stores that have multiple versions of their data set.
 *
 * Currently, the class only supports the following functionality:
 * 1. Enabling/disabling specific store versions,
 * 2. Keeping track of which version is the current one.
 *
 * Note that 1 and 2 are orthogonal: the current version can be enabled or disabled.
 *
 * TODO: Port atomic swap functionality here.
 * TODO: Port delete backups functionality here.
 *
 * Eventually, this can allow us to generically manage stores with multiple data set
 * versions (not just ReadOnly).
 */
public class StoreVersionManager {
    private static final Logger logger = Logger.getLogger(StoreVersionManager.class);

    private static final String DISABLED_MARKER_NAME = ".disabled";

    private final File rootDir;
    private final String storeName;
    private final Map<Long, Boolean> versionToEnabledMap = Maps.newConcurrentMap();
    private final VoldemortConfig config;
    private long currentVersion;

    /**
     * This constructor inspects the rootDir of the store and finds out which
     * versions exist and which one is active.
     *
     * @param rootDir of the store to be managed.
     */
    public StoreVersionManager(File rootDir, VoldemortConfig config) {
        this.rootDir = rootDir;
        this.config = config;

        this.storeName = rootDir.getName();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        sb.append(" { currentVersion: ");
        sb.append(currentVersion);
        sb.append(", versionToEnabledMap: {");
        boolean firstItem = true;
        for (Map.Entry<Long, Boolean> entry: versionToEnabledMap.entrySet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append(entry.getKey());
            sb.append(": ");
            sb.append(entry.getValue());
        }
        sb.append("}, rootDir: ");
        sb.append(rootDir);
        sb.append(" }");
        return sb.toString();
    }

    /**
     * Compares the StoreVersionManager's internal state with the content on the file-system
     * of the rootDir provided at construction time.
     *
     * TODO: If the StoreVersionManager supports non-RO stores in the future,
     *       we should move some of the ReadOnlyUtils functions below to another Utils class.
     */
    public void syncInternalStateFromFileSystem(boolean alsoSyncRemoteState) {
        // Make sure versions missing from the file-system are cleaned up from the internal state
        for (Long version: versionToEnabledMap.keySet()) {
            File[] existingVersionDirs = ReadOnlyUtils.getVersionDirs(rootDir, version, version);
            if (existingVersionDirs.length == 0) {
                removeVersion(version, alsoSyncRemoteState);
            }
        }

        // Make sure we have all versions on the file-system in the internal state
        File[] versionDirs = ReadOnlyUtils.getVersionDirs(rootDir);
        if (versionDirs != null) {
            for (File versionDir: versionDirs) {
                long versionNumber = ReadOnlyUtils.getVersionId(versionDir);
                boolean versionEnabled = isVersionEnabled(versionDir);
                versionToEnabledMap.put(versionNumber, versionEnabled);
            }
        }

        // Identify the current version (based on a symlink in the file-system)
        File currentVersionDir = ReadOnlyUtils.getCurrentVersion(rootDir);
        if (currentVersionDir != null) {
            currentVersion = ReadOnlyUtils.getVersionId(currentVersionDir);
        } else {
            currentVersion = -1; // Should we throw instead?
        }

        logger.info("Successfully synced internal state from local file-system: " + this.toString());
    }

    /**
     * Enables a specific store/version, so that it stops failing requests.
     *
     * @param version to be enabled
     */
    public void enableStoreVersion(long version) throws PersistenceFailureException {
        versionToEnabledMap.put(version, true);
        persistEnabledVersion(version);
    }

    /**
     * Disables a specific store/version. When disabled, a store should
     * fail all subsequent requests to it.
     *
     * @param version to be disabled
     */
    public void disableStoreVersion(long version) throws PersistenceFailureException {
        versionToEnabledMap.put(version, false);
        persistDisabledVersion(version);
    }

    /**
     * Tells whether a version is enabled or disabled.
     *
     * @param version which we want to know the status of.
     * @return true if the requested version is enabled,
     *         false if the requested version is disabled,
     *         null if the requested version does not exist
     */
    public Boolean isStoreVersionEnabled(long version) {
        return versionToEnabledMap.get(version);
    }

    /**
     * Tells whether the current version is enabled or disabled.
     *
     * @return true if the requested version is enabled,
     *         false if the requested version is disabled,
     *         null if the requested version does not exist
     */
    public Boolean isCurrentVersionEnabled() {
        return isStoreVersionEnabled(currentVersion);
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(long currentVersion) {
        this.currentVersion = currentVersion;
    }

    public boolean hasAnyDisabledVersion() {
        for (Boolean enabled: versionToEnabledMap.values()) {
            if (!enabled) return true;
        }
        return false;
    }

    // PRIVATE UTILITY FUNCTIONS

    /**
     * Inspects the specified versionDir to see if it has been marked as disabled
     * (via a .disabled file in the directory). If the file is absent, the store is
     * assumed to be enabled.
     *
     * @param versionDir to inspect
     * @return true if the specified version is enabled, false otherwise
     * @throws IllegalArgumentException if the version does not exist
     */
    private boolean isVersionEnabled(File versionDir) throws IllegalArgumentException {
        if (!versionDir.exists()) {
            throw new IllegalArgumentException("The versionDir " + versionDir.getName() + " does not exist.");
        }
        File[] relevantFile = versionDir.listFiles(new FileFilter() {
            public boolean accept(File pathName) {
                return pathName.getName().equals(DISABLED_MARKER_NAME);
            }
        });
        return relevantFile.length == 0;
    }

    /**
     * Places a disabled marker file in the directory of the specified version.
     *
     * @param version to disable
     * @throws PersistenceFailureException if the marker file could not be created (can happen if
     *                                     the storage system has become read-only or is otherwise
     *                                     inaccessible).
     */
    private void persistDisabledVersion(long version) throws PersistenceFailureException {
        File disabledMarker = getDisabledMarkerFile(version);
        try {
            disabledMarker.createNewFile();
        } catch (IOException e) {
            throw new PersistenceFailureException("Failed to create the disabled marker at path: " +
                                                  disabledMarker.getAbsolutePath() + "\nThe store/version " +
                                                  "will remain disabled only until the next restart.", e);
        }
    }

    /**
     * Deletes the disabled marker file in the directory of the specified version.
     *
     * @param version to enable
     * @throws PersistenceFailureException if the marker file could not be deleted (can happen if
     *                                     the storage system has become read-only or is otherwise
     *                                     inaccessible).
     */
    private void persistEnabledVersion(long version) throws PersistenceFailureException {
        File disabledMarker = getDisabledMarkerFile(version);
        if (disabledMarker.exists()) {
            if (!disabledMarker.delete()) {
                throw new PersistenceFailureException("Failed to create the disabled marker at path: " +
                                                      disabledMarker.getAbsolutePath() + "\nThe store/version " +
                                                      "will remain enabled only until the next restart.");
            }
        }
    }

    /**
     * Gets the '.disabled' file for a given version of this store. That file may or may not
     * exist.
     *
     * @param version of the store for which to get the '.disabled' file.
     * @return an instance of {@link File} pointing to the '.disabled' file.
     * @throws PersistenceFailureException if the requested version cannot be found.
     */
    private File getDisabledMarkerFile(long version) throws PersistenceFailureException {
        File[] versionDirArray = ReadOnlyUtils.getVersionDirs(rootDir, version, version);
        if (versionDirArray.length == 0) {
            throw new PersistenceFailureException("getDisabledMarkerFile did not find the requested version directory" +
                                                  " on disk. Version: " + version + ", rootDir: " + rootDir);
        }
        File disabledMarkerFile = new File(versionDirArray[0], DISABLED_MARKER_NAME);
        return disabledMarkerFile;
    }

    private void removeVersion(long version, boolean alsoSyncRemoteState) {
        if (currentVersion == version) {
            currentVersion = -1; // Should we throw instead?
        }
        versionToEnabledMap.remove(version);

        if (alsoSyncRemoteState && config != null && config.getHighAvailabilityStateAutoCleanUp()) {
            FailedFetchLock failedFetchLock = null;
            try {
                failedFetchLock = FailedFetchLock.getLock(config, new Props());
                removeRemoteObsoleteState(failedFetchLock);
            } catch (Exception e) {
                logger.error("Failed to execute failedFetchLock.removeObsoleteStateForStore() for store " + storeName, e);
            } finally {
                IOUtils.closeQuietly(failedFetchLock);
            }
        }
    }

    public void removeRemoteObsoleteState(FailedFetchLock failedFetchLock) {
        if (config != null && config.getHighAvailabilityStateAutoCleanUp()) {
            try {
                failedFetchLock.removeObsoleteStateForStore(config.getNodeId(), storeName, versionToEnabledMap);
            } catch (Exception e) {
                logger.error("Failed to execute failedFetchLock.removeObsoleteStateForStore() for store " + storeName, e);
            }
            logger.info("Successfully synced internal state with remote FailedFetchLock state for store " + storeName);
        } else {
            logger.debug("Will not attempt removeRemoteObsoleteState() because it is disabled. " +
                    "This can be enabled with " + VoldemortConfig.PUSH_HA_STATE_AUTO_CLEANUP + "=true");
        }
    }
}
