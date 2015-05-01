package voldemort.store.readonly.swapper;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.utils.Props;

import java.io.IOException;
import java.util.Set;

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
    private final static String IO_EXCEPTION = "of an IOException";
    private final static String ALREADY_EXISTS = "it is already acquired (most likely)";
    private final static String UNKNOWN_REASONS = "of unknown reasons";

    private final static String PUSH_HA_LOCK_HDFS_TIMEOUT = "push.ha.lock.hdfs.timeout";
    private final static String PUSH_HA_LOCK_HDFS_RETRIES = "push.ha.lock.hdfs.retries";
    private final static String PUSH_HA_LOCK_HDFS_PATH = "push.ha.lock.hdfs.path";

    private final static String AZKABAN_FLOW_ID = "azkaban.flow.flowid";
    private final static String AZKABAN_JOB_ID = "azkaban.job.id";
    private final static String AZKABAN_EXEC_ID = "azkaban.flow.execid";

    // Azkaban state
    private final String flowId = props.getString(AZKABAN_FLOW_ID);
    private final String jobId = props.getString(AZKABAN_JOB_ID);
    private final String execId = props.getString(AZKABAN_EXEC_ID);

    // Default total try time = 10000 ms timeOut * 360 maxAttempts = 15 minutes
    private final Integer waitBetweenRetries = props.getInt(PUSH_HA_LOCK_HDFS_TIMEOUT, 10000);
    private final Integer maxAttempts = props.getInt(PUSH_HA_LOCK_HDFS_RETRIES, 90);

    // HDFS directories
    private final String baseDir = props.getString(PUSH_HA_LOCK_HDFS_PATH);
    private final String clusterDir = baseDir + "/" + clusterId;
    private final String lockDir = clusterDir + "/" + LOCK_DIR;
    private final String beforeLockDir = lockDir + "/" + BEFORE_ACQUISITION_DIR;
    private final String afterLockDir = lockDir + "/" + AFTER_RELEASE_DIR;

    // HDFS
    private final Path lockFile = new Path(clusterDir, LOCK_NAME);
    private final Path clusterPath = new Path(clusterDir);
    private final FileSystem fileSystem;

    // Internal State
    private boolean lockAcquired = false;

    public HdfsFailedFetchLock(Props props, String clusterUrl) throws Exception {
        super(props, clusterUrl);
        fileSystem = clusterPath.getFileSystem(new Configuration());
        initDirs();
    }

    private void initDirs() throws Exception {
        int attempts = 1;
        boolean success = false;
        while (!success && attempts <= maxAttempts) {
            try {
                success = this.fileSystem.mkdirs(new Path(afterLockDir));

                if (!success) {
                    logger.warn(logMessage(INIT_DIRS, UNKNOWN_REASONS, attempts));
                }
            }  catch (IOException e) {
                handleIOException(e, INIT_DIRS, attempts);
                wait(waitBetweenRetries);
                attempts++;
            }
        }

        if (!success) {
            throw new VoldemortException(exceptionMessage(INIT_DIRS));
        }
    }

    private String getUniqueFileName() {
        return "Exec" + execId + "-Time" + System.currentTimeMillis() + "-" + flowId + "-" + jobId;
    }

    private String logMessage(String action, String cause, int attempt) {
        return "Failed to " + action + " because " + cause + ". Attempt # " +
                attempt + "/" + maxAttempts + ", will wait " +
                waitBetweenRetries + " ms until next retry.";
    }

    private String exceptionMessage(String action) {
        return "Failed to " + action + " after " + maxAttempts + " attempts.";
    }

    /**
     * This function is intended to detect the subset of IOException which are not
     * considered recoverable, in which case we want to bubble up the exception, instead
     * of retrying.
     *
     * @param e
     * @throws VoldemortException
     */
    private void handleIOException(IOException e, String action, int attempt) throws VoldemortException {
        if (e.getMessage().contains("Filesystem closed")) {
            throw new VoldemortException("Got an IOException we cannot recover from while trying to " +
                    action + ". Attempt # " + attempt + "/" + maxAttempts + ". Will not try again.", e);
        } else {
            logger.error(logMessage(action, IO_EXCEPTION, attempt), e);
        }
    }

    @Override
    public synchronized void acquireLock() throws Exception {
        if (lockAcquired) {
            logger.info("HdfsFailedFetchLock.acquireLock() called while it is already acquired!");
        } else {
            int attempts = 1;
            while (!this.lockAcquired && attempts <= maxAttempts) {
                FSDataOutputStream outputStream = null;
                try {
                    // We prepare a temporaryLockFile with the content we want in a path without locking.
                    Path temporaryLockFile = new Path(beforeLockDir, getUniqueFileName());
                    outputStream = this.fileSystem.create(temporaryLockFile, false);
                    props.storeFlattened(outputStream);
                    outputStream.flush();
                    outputStream.close();

                    // We attempt to rename to the globally contended lock path
                    this.lockAcquired = this.fileSystem.rename(temporaryLockFile, this.lockFile);

                    if (!this.lockAcquired) {
                        logger.warn(logMessage(ACQUIRE_LOCK, ALREADY_EXISTS, attempts));
                        this.fileSystem.delete(temporaryLockFile, false);
                    }
                }  catch (IOException e) {
                    handleIOException(e, ACQUIRE_LOCK, attempts);
                } finally {
                    if (outputStream != null) {
                        // Just being paranoid...
                        outputStream.close();
                    }
                }

                if (!this.lockAcquired) {
                    wait(waitBetweenRetries);
                    attempts++;
                }
            }

            if (!this.lockAcquired) {
                throw new VoldemortException(exceptionMessage(ACQUIRE_LOCK));
            }
        }
    }

    @Override
    public synchronized void releaseLock() throws Exception {
        if (!lockAcquired) {
            logger.info("HdfsFailedFetchLock.releaseLock() called while it is already released!");
        } else {
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
                        logger.warn(logMessage(RELEASE_LOCK, UNKNOWN_REASONS, attempts));
                    }
                }  catch (IOException e) {
                    handleIOException(e, RELEASE_LOCK, attempts);
                }

                if (this.lockAcquired) {
                    wait(waitBetweenRetries);
                    attempts++;
                }
            }

            if (this.lockAcquired) {
                throw new VoldemortException(exceptionMessage(RELEASE_LOCK));
            }
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
            }  catch (IOException e) {
                handleIOException(e, GET_DISABLED_NODES, attempts);
                wait(waitBetweenRetries);
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
                                String details,
                                String storeName,
                                long storeVersion) throws Exception {
        if (!lockAcquired) {
            throw new VoldemortException("HdfsFailedFetchLock.addDisabledNode() called while the lock is not acquired!");
        } else {
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
                    wait(waitBetweenRetries);
                    attempts++;
                } finally {
                    if (outputStream != null) {
                        // Just being paranoid...
                        outputStream.close();
                    }
                }
            }

            if (!success) {
                throw new VoldemortException(exceptionMessage(ADD_DISABLED_NODE));
            }
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
        this.fileSystem.close();
    }
}
