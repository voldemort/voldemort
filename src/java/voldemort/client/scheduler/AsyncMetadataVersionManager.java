/*
 * Copyright 2008-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.scheduler;

import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import voldemort.client.SystemStoreRepository;
import voldemort.utils.MetadataVersionStoreUtils;

/**
 * The AsyncMetadataVersionManager is used to track the Metadata version on the
 * cluster and if necessary Re-bootstrap the client.
 * 
 * During initialization, it will retrieve the current version of the
 * cluster.xml and then periodically check whether this has been updated. During
 * init if the initial version turns out to be null, it means that no change has
 * been done to that store since it was created. In this case, we assume version
 * '0'.
 * 
 * At the moment, this only tracks the cluster.xml changes. TODO: Extend this to
 * track other stuff (like stores.xml)
 * 
 * @author csoman
 * 
 */

public class AsyncMetadataVersionManager implements Runnable {

    public static final String CLUSTER_VERSION_KEY = "cluster.xml";
    public static String STORES_VERSION_KEY = "stores.xml";
    public static final String VERSIONS_METADATA_STORE = "metadata-versions";

    private final Logger logger = Logger.getLogger(this.getClass());
    private Long currentClusterVersion;
    private Long currentStoreVersion;
    private final Callable<Void> storeClientThunk;
    private final SystemStoreRepository systemStoreRepository;
    public boolean isActive = false;

    public AsyncMetadataVersionManager(SystemStoreRepository sysRepository,
                                       Callable<Void> storeClientThunk,
                                       String storeName) {
        this.systemStoreRepository = sysRepository;

        if(storeName != null) {
            STORES_VERSION_KEY = storeName;
        }

        // Get the properties object from the system store (containing versions)
        Properties versionProps = MetadataVersionStoreUtils.getProperties(this.systemStoreRepository.getMetadataVersionStore());

        // Initialize base cluster version to do all subsequent comparisons
        this.currentClusterVersion = initializeVersion(CLUSTER_VERSION_KEY, versionProps);

        // Initialize base store version to do all subsequent comparisons
        this.currentStoreVersion = initializeVersion(STORES_VERSION_KEY, versionProps);

        logger.debug("Initial cluster.xml version = " + this.currentClusterVersion);
        logger.debug("Initial store '" + storeName + "' version = " + this.currentClusterVersion);

        this.storeClientThunk = storeClientThunk;
        this.isActive = true;
    }

    private Long initializeVersion(String versionKey, Properties versionProps) {
        Long baseVersion = null;
        try {
            baseVersion = getCurrentVersion(versionKey, versionProps);
        } catch(Exception e) {
            logger.error("Exception while getting version for key : " + versionKey
                         + " Exception : " + e);
        }

        if(baseVersion == null) {
            baseVersion = new Long(0);
        }
        return baseVersion;
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

                /*
                 * Check if the new version is greater than the current one. We
                 * should not re-bootstrap on a stale version.
                 */
                if(newVersion > curVersion) {
                    return newVersion;
                }
            } else {
                logger.debug("Metadata unchanged after creation ...");
            }
        }

        // Swallow all exceptions here (we dont want to fail the client).
        catch(Exception e) {
            logger.debug("Could not retrieve Metadata Version. Exception : " + e);
        }

        return null;
    }

    public void run() {

        try {
            /*
             * Get the properties object from the system store (containing
             * versions)
             */
            Properties versionProps = MetadataVersionStoreUtils.getProperties(this.systemStoreRepository.getMetadataVersionStore());

            Long newClusterVersion = fetchNewVersion(CLUSTER_VERSION_KEY,
                                                     this.currentClusterVersion,
                                                     versionProps);
            Long newStoreVersion = fetchNewVersion(STORES_VERSION_KEY,
                                                   this.currentStoreVersion,
                                                   versionProps);

            // Check if something has been updated
            if((newClusterVersion != null) || (newStoreVersion != null)) {
                logger.info("Metadata version mismatch detected. Re-bootstrapping!");
                try {
                    if(newClusterVersion != null) {
                        logger.info("Updating cluster version");
                        currentClusterVersion = newClusterVersion;
                    }

                    if(newStoreVersion != null) {
                        logger.info("Updating store : '" + STORES_VERSION_KEY + "' version");
                        this.currentStoreVersion = newStoreVersion;
                    }

                    this.storeClientThunk.call();

                } catch(Exception e) {
                    if(logger.isDebugEnabled()) {
                        e.printStackTrace();
                        logger.debug(e.getMessage());
                    }
                }
            }

        } catch(Exception e) {
            logger.debug("Could not retrieve metadata versions from the server.");
        }

    }

    public Long getClusterMetadataVersion() {
        return this.currentClusterVersion;
    }

    public Long getStoreMetadataVersion() {
        return this.currentStoreVersion;
    }

    // Fetch the latest versions for cluster metadata
    public void updateMetadataVersions() {
        Properties versionProps = MetadataVersionStoreUtils.getProperties(this.systemStoreRepository.getMetadataVersionStore());
        Long newVersion = fetchNewVersion(CLUSTER_VERSION_KEY, null, versionProps);
        if(newVersion != null) {
            this.currentClusterVersion = newVersion;
        }
    }
}
