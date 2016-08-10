package voldemort.store.readonly.swapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.utils.ExceptionUtils;
import voldemort.utils.Props;

import com.google.common.collect.Sets;

/**
 * An implementation of the {@link FailedFetchLock} that uses HDFS as a global lock.
 *
 * This lock acts upon a directory structure in HDFS which is segmented by clusters:
 *
 *      hdfs://namenode:port/baseDir/clusterId/
 *
 * Within each cluster, there is a lock file which allows sensitive operations to be
 * synchronized within a single cluster, independently of the other clusters. The
 * content of the lock file is the props passed to the lock's constructor. The
 * lock file is stored at:
 *
 *      hdfs://namenode:port/baseDir/clusterId/bnp.lock
 *
 * Within each cluster, there is also a node directory created for each disabled node:
 *
 *      hdfs://namenode:port/baseDir/clusterId/nodeId/
 *
 * Within each node directory, there is a directory for each store which has been disabled:
 *
 *      hdfs://namenode:port/baseDir/clusterId/nodeId/storeName/
 *
 * Within each store directory, there is a directory for each version of the store which
 * failed to fetch and thus caused the store to become disabled:
 *
 *      hdfs://namenode:port/baseDir/clusterId/nodeId/storeName/storeVersion/
 *
 * Within each store version directory, there is a file for each BnP run which tried to
 * push this store/version. Normally, there should be only one such file, but in some
 * scenarios, there might be more than one. The name of the file is the time at which
 * the store was disabled, and its content is the props passed to the lock's
 * constructor. The BnP run file is stored here:
 *
 *      hdfs://namenode:port/baseDir/clusterId/nodeId/storeName/storeVersion/unique-name
 *
 */
public class HdfsFailedFetchLock extends FailedFetchLock {
    private final static Logger logger = Logger.getLogger(HdfsFailedFetchLock.class);

    // Constants
    private final static String LOCK_NAME = "bnp.lock";
    private final static String LOCK_DIR = "locks";
    private final static String BEFORE_ACQUISITION_DIR = "before-lock-acquisition";
    private final static String AFTER_RELEASE_DIR = "after-lock-release";
    private final static String NODE_ID_DIR_PREFIX = "node-";

    private final static String INIT_DIRS = "initialize HDFS directories";
    private final static String ACQUIRE_LOCK = "acquire HDFS lock";
    private final static String RELEASE_LOCK = "release HDFS lock";
    private final static String GET_DISABLED_NODES = "retrieve disabled nodes from HDFS";
    private final static String ADD_DISABLED_NODE = "add a disabled node in HDFS";
    private final static String CLEAR_OBSOLETE_STATE = "clear obsolete state in HDFS";
    private final static String IO_EXCEPTION = "of an IOException";
    private final static String ALREADY_EXISTS = "it is already acquired (most likely)";
    private final static String UNKNOWN_REASONS = "of unknown reasons";

    private final static String PUSH_HA_LOCK_HDFS_TIMEOUT = "push.ha.lock.hdfs.timeout";
    private final static String PUSH_HA_LOCK_HDFS_RETRIES = "push.ha.lock.hdfs.retries";

    private final static String AZKABAN_FLOW_ID = "azkaban.flow.flowid";
    private final static String AZKABAN_JOB_ID = "azkaban.job.id";
    private final static String AZKABAN_EXEC_ID = "azkaban.flow.execid";

    // Azkaban state
    private final String flowId = props.getString(AZKABAN_FLOW_ID, "null." + AZKABAN_FLOW_ID);
    private final String jobId = props.getString(AZKABAN_JOB_ID, "null." + AZKABAN_JOB_ID);
    private final String execId = props.getString(AZKABAN_EXEC_ID, "null." + AZKABAN_EXEC_ID);

    // Default total try time = 10000 ms timeOut * 360 maxAttempts = 15 minutes
    private final Integer waitBetweenRetries = props.getInt(PUSH_HA_LOCK_HDFS_TIMEOUT, 10000);
    private final Integer maxAttempts = props.getInt(PUSH_HA_LOCK_HDFS_RETRIES, 90);

    // HDFS directories
    private final String clusterDir = lockPath + "/" + clusterId;
    private final String lockDir = clusterDir + "/" + LOCK_DIR;
    private final String beforeLockDir = lockDir + "/" + BEFORE_ACQUISITION_DIR;
    private final String afterLockDir = lockDir + "/" + AFTER_RELEASE_DIR;

    // HDFS
    private final Path lockFile = new Path(clusterDir, LOCK_NAME);
    private final Path clusterPath = new Path(clusterDir);
    private final Path afterLockPath = new Path(afterLockDir);
    private final FileSystem fileSystem;

    // Internal State
    private boolean lockAcquired = false;

    public HdfsFailedFetchLock(VoldemortConfig config, Props props) throws Exception {
        super(config, props);
        fileSystem = HadoopUtils.getHadoopFileSystem(config, clusterDir);
        try {
            initDirs();
        } catch(Exception ex) {
            IOUtils.closeQuietly(this.fileSystem);
            throw ex;
        }
    }

    private void initDirs() throws Exception {
        int attempts = 1;
        boolean success = false;
        while (!success && attempts <= maxAttempts) {
            try {
                success = this.fileSystem.mkdirs(afterLockPath);

                if (!success) {
                    logFailureAndWait(INIT_DIRS, UNKNOWN_REASONS, attempts);
                }
            }  catch (IOException e) {
                handleIOException(e, INIT_DIRS, attempts);
            } finally {
                if (!success) {
                    attempts++;
                }
            }
        }

        if (!success) {
            throw new VoldemortException(exceptionMessage(INIT_DIRS));
        }
    }

    private String getUniqueFileName() {
        return "Exec" + execId + "-Time" + System.currentTimeMillis() + "-" + flowId + "-" + jobId;
    }

    private void logFailureAndWait(String action, String cause, int attempt) throws InterruptedException {
        logFailureAndWait(action, cause, attempt, null);
    }

    private void logFailureAndWait(String action, String cause, int attempt, Exception e) throws InterruptedException {
        String retryMessage;
        if (attempt < maxAttempts) {
            retryMessage = ", will wait " + waitBetweenRetries + " ms until next retry.";
        } else {
            retryMessage = ", no further attempts will be performed.";
        }
        String fullMessage = "Failed to " + action + " because " + cause + ". Attempt #" +
                attempt + "/" + maxAttempts + retryMessage;

        if (e == null) {
            logger.warn(fullMessage);
        } else {
            logger.error(fullMessage, e);
        }

        Thread.sleep(waitBetweenRetries);
    }

    private String exceptionMessage(String action) {
        return "Failed to " + action + " after " + maxAttempts + " attempts.";
    }

    /**
     * This function is intended to detect the subset of IOException which are not
     * considered recoverable, in which case we want to bubble up the exception, instead
     * of retrying.
     *
     * @throws VoldemortException
     */
    private void handleIOException(IOException e, String action, int attempt)
            throws VoldemortException, InterruptedException {
        if ( // any of the following happens, we need to bubble up
                // FileSystem instance got closed, somehow
                e.getMessage().contains("Filesystem closed") ||
                // HDFS permission issues
                ExceptionUtils.recursiveClassEquals(e, AccessControlException.class)) {
            throw new VoldemortException("Got an IOException we cannot recover from while trying to " +
                    action + ". Attempt # " + attempt + "/" + maxAttempts + ". Will not try again.", e);
        } else {
            logFailureAndWait(action, IO_EXCEPTION, attempt, e);
        }
    }

    @Override
    public synchronized void acquireLock() throws Exception {
        logger.info("Try to acquire HDFS distributed lock.");
        if (lockAcquired) {
            logger.info("HdfsFailedFetchLock.acquireLock() called while it is already acquired!");
            return;
        }

        int attempts = 1;
        while (!this.lockAcquired && attempts <= maxAttempts) {
            FSDataOutputStream outputStream = null;
            try {
                // We prepare a temporaryLockFile with the content we want in a path without locking.
                Path temporaryLockFile = new Path(beforeLockDir, getUniqueFileName());
                outputStream = this.fileSystem.create(temporaryLockFile, false);
                props.storeFlattened(outputStream);
                outputStream.flush();
                IOUtils.closeQuietly(outputStream); // necessary, otherwise the rename fails.

                // We attempt to rename to the globally contended lock path
                this.lockAcquired = this.fileSystem.rename(temporaryLockFile, this.lockFile);

                if (!this.lockAcquired) {
                    logFailureAndWait(ACQUIRE_LOCK, ALREADY_EXISTS, attempts);
                    this.fileSystem.delete(temporaryLockFile, false);
                }
            } catch (IOException e) {
                handleIOException(e, ACQUIRE_LOCK, attempts);
            } finally {
                IOUtils.closeQuietly(outputStream); // in case we failed before the other close above.
            }

            if (!this.lockAcquired) {
                attempts++;
            }
        }

        if (!this.lockAcquired) {
            throw new VoldemortException(exceptionMessage(ACQUIRE_LOCK));
        }
        logger.info("HDFS distributed lock acquired.");
    }

    @Override
    public synchronized void releaseLock() throws Exception {
        if (!lockAcquired) {
            logger.info("HdfsFailedFetchLock.releaseLock() called while it is already released!");
            return;
        }

        int attempts = 1;
        while (this.lockAcquired && attempts <= maxAttempts) {
            try {
                // We prepare a releasedLockFile in a path without locking. This is to keep the lock
                // file's content for traceability purposes, so we can know which locks have been
                // acquired in the past...
                Path releasedLockFile = new Path(afterLockDir, getUniqueFileName());

                // We attempt to rename the globally contended lock path to the the released path.
                this.lockAcquired = !(this.fileSystem.rename(this.lockFile, releasedLockFile));

                if (this.lockAcquired) {
                    logFailureAndWait(RELEASE_LOCK, UNKNOWN_REASONS, attempts);
                }
            }  catch (IOException e) {
                handleIOException(e, RELEASE_LOCK, attempts);
            }

            if (this.lockAcquired) {
                attempts++;
            }
        }

        if (this.lockAcquired) {
            throw new VoldemortException(exceptionMessage(RELEASE_LOCK));
        }
    }

    @Override
    public Set<Integer> getDisabledNodes() throws Exception {
        Set<Integer> disabledNodes = null;
        int attempts = 1;
        while (disabledNodes == null && attempts <= maxAttempts) {
            try {
                FileStatus[] fileStatusArray = this.fileSystem.listStatus(clusterPath);

                disabledNodes = Sets.newHashSet();

                for (FileStatus fileStatus: fileStatusArray) {
                    String fileName = fileStatus.getPath().getName();
                    if (fileName.startsWith(NODE_ID_DIR_PREFIX)) {
                        Integer nodeId = Integer.parseInt(fileName.substring(NODE_ID_DIR_PREFIX.length()));
                        disabledNodes.add(nodeId);
                    }
                }
            } catch (IOException e) {
                handleIOException(e, GET_DISABLED_NODES, attempts);
                attempts++;
            }
        }

        if (disabledNodes == null) {
            throw new VoldemortException(exceptionMessage(GET_DISABLED_NODES));
        } else {
            return disabledNodes;
        }
    }

    @Override
    public void addDisabledNode(int nodeId,
                                String storeName,
                                long storeVersion) throws Exception {
        if (!lockAcquired) {
            throw new VoldemortException("HdfsFailedFetchLock.addDisabledNode() called while the lock is not acquired!");
        }

        int attempts = 1;
        boolean success = false;
        while (!success && attempts <= maxAttempts) {
            FSDataOutputStream outputStream = null;
            try {
                String nodeIdDir = NODE_ID_DIR_PREFIX + nodeId;
                String failedJobDir = clusterDir + "/" + nodeIdDir + "/" + storeName + "/" + storeVersion;
                Path failedJobFile = new Path(failedJobDir, getUniqueFileName());

                FileUtil.copy(this.fileSystem, this.lockFile, this.fileSystem, failedJobFile, false, true, new Configuration());

                success = true;
            }  catch (IOException e) {
                handleIOException(e, ADD_DISABLED_NODE, attempts);
                attempts++;
            } finally {
                IOUtils.closeQuietly(outputStream);
            }
        }

        if (!success) {
            throw new VoldemortException(exceptionMessage(ADD_DISABLED_NODE));
        }
    }

    @Override
    public void removeObsoleteStateForNode(int nodeId) throws Exception {
        int attempts = 1;
        boolean success = false;
        while (!success && attempts <= maxAttempts) {
            try {
                String nodeIdDir = NODE_ID_DIR_PREFIX + nodeId;
                Path failedNodePath = new Path(clusterDir + "/" + nodeIdDir);;

                if (this.fileSystem.exists(failedNodePath)) {
                    this.fileSystem.delete(failedNodePath, true);
                    logger.info("The BnP HA shared state has been cleared for node: " + nodeId);
                } else {
                    logger.info("No-op. The BnP HA shared state already has no directory for node: " + nodeId);
                }

                success = true;
            } catch (IOException e) {
                handleIOException(e, CLEAR_OBSOLETE_STATE, attempts);
                attempts++;
            }
        }

        if (!success) {
            throw new VoldemortException(exceptionMessage(CLEAR_OBSOLETE_STATE));
        }

    }

    @Override
    public void removeObsoleteStateForStore(int nodeId, String storeName, Map<Long, Boolean> versionToEnabledMap) throws Exception {
        int attempts = 1;
        boolean success = false;
        while (!success && attempts <= maxAttempts) {
            try {
                String nodeIdDir = NODE_ID_DIR_PREFIX + nodeId;
                Path failedStorePath = new Path(clusterDir + "/" + nodeIdDir + "/" + storeName);

                FileStatus[] disabledVersions;
                try {
                    disabledVersions = this.fileSystem.listStatus(failedStorePath);
                    for (FileStatus disabledVersionDir: disabledVersions) {
                        Long disabledVersion = Long.parseLong(disabledVersionDir.getPath().getName());
                        Boolean isDisabledLocally = versionToEnabledMap.get(disabledVersion);
                        if (isDisabledLocally == null || !isDisabledLocally) {
                            logger.info("The shared state has an obsolete disabled stored version which we will delete: " +
                                    disabledVersionDir.getPath().toString());
                            this.fileSystem.delete(disabledVersionDir.getPath(), true);
                        }
                    }

                    // Let's see if there's still anything left for this store?
                    disabledVersions = this.fileSystem.listStatus(failedStorePath);
                    if (disabledVersions.length == 0) {
                        logger.info("There are no more disabled versions for this store, so we will delete: " +
                                failedStorePath.toString());
                        this.fileSystem.delete(failedStorePath, true);
                    }
                } catch (FileNotFoundException e) {
                    logger.info("The shared state has no obsolete versions in: " + failedStorePath.toString());
                }

                // Let's see if there's still anything left for this node?
                Path disabledStoresPath = new Path(clusterDir + "/" + nodeIdDir);
                try {
                    FileStatus[] disabledStores = this.fileSystem.listStatus(disabledStoresPath);
                    if (disabledStores.length == 0) {
                        logger.info("There are no more disabled stores for this node, so we will delete: " +
                                disabledStoresPath.toString());
                        this.fileSystem.delete(disabledStoresPath, true);
                    }
                } catch (FileNotFoundException e) {
                    logger.info("The shared state has no obsolete stores in: " + disabledStoresPath.toString());
                }

                success = true;
            }  catch (IOException e) {
                handleIOException(e, CLEAR_OBSOLETE_STATE, attempts);
                attempts++;
            }
        }

        if (!success) {
            throw new VoldemortException(exceptionMessage(CLEAR_OBSOLETE_STATE));
        }
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * @throws java.io.IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        try {
            releaseLock();
        } catch (Exception e) {
            logger.error("Got an exception during close()", e);
        }
        IOUtils.closeQuietly(this.fileSystem);
    }
}
