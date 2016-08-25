/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.readonly.fetcher;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.ObjectName;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AdminServiceRequestHandler;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.UnauthorizedStoreException;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.store.readonly.mr.utils.VoldemortUtils;
import voldemort.utils.ByteUtils;
import voldemort.utils.EventThrottler;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/**
 * A {@link FileFetcher} implementation that fetches the store files from HDFS
 */
public class HdfsFetcher implements FileFetcher {

    // Class-level state
    private static final Logger logger = Logger.getLogger(HdfsFetcher.class);
    private static final AtomicInteger copyCount = new AtomicInteger(0);
    private static Boolean allowFetchingOfSingleFile = false;

    // Instance-level state
    private final Long maxBytesPerSecond, reportingIntervalBytes;
    private final int bufferSize, maxAttempts, maxVersionsStatsFile;
    private final long retryDelayMs;
    private final boolean enableStatsFile;
    private final EventThrottler throttler;
    private final VoldemortConfig voldemortConfig;

    /**
     * This is the constructor invoked via reflection from
     * {@link AdminServiceRequestHandler#setFetcherClass(voldemort.server.VoldemortConfig)}
     */
    public HdfsFetcher(VoldemortConfig config) {
        this(config,
             config.getReadOnlyFetcherMaxBytesPerSecond(),
             config.getReadOnlyFetcherReportingIntervalBytes(),
             config.getReadOnlyFetcherThrottlerInterval(),
             config.getFetcherBufferSize(),
             config.getReadOnlyFetchRetryCount(),
             config.getReadOnlyFetchRetryDelayMs(),
             config.isReadOnlyStatsFileEnabled(),
             config.getReadOnlyMaxVersionsStatsFile(),
             config.getFetcherSocketTimeout());
    }


    /**
     * Test-only constructor, with some default config values, including:
     *
     * The Hadoop config path is set to empty string, which triggers a different code path
     * in {@link HadoopUtils#getConfiguration(voldemort.server.VoldemortConfig, String)}.
     *
     * The keytab path is set to empty string, which triggers a different code path
     * in {@link HadoopUtils#getHadoopFileSystem(voldemort.server.VoldemortConfig, String)}.
     *
     * FIXME: Change visibility or otherwise ensure that only test code can use this...
     *
     * @deprecated Do not use for production code, use {@link #HdfsFetcher(voldemort.server.VoldemortConfig)} instead.
     */
    @Deprecated
    public HdfsFetcher() {
        this(new VoldemortConfig(-1, ""), // Fake config with a bogus node ID and server config path
             (Long) null,
             VoldemortConfig.DEFAULT_REPORTING_INTERVAL_BYTES,
             VoldemortConfig.DEFAULT_FETCHER_THROTTLE_INTERVAL_WINDOW_MS,
             VoldemortConfig.DEFAULT_FETCHER_BUFFER_SIZE,
             3,
             1000,
             true,
             50,
             VoldemortConfig.DEFAULT_FETCHER_SOCKET_TIMEOUT);
        this.voldemortConfig.setHadoopConfigPath("");
        this.voldemortConfig.setReadOnlyKeytabPath("");
    }

    private HdfsFetcher(VoldemortConfig config,
                        Long maxBytesPerSecond,
                        Long reportingIntervalBytes,
                        int throttlerIntervalMs,
                        int bufferSize,
                        int retryCount,
                        long retryDelayMs,
                        boolean enableStatsFile,
                        int maxVersionsStatsFile,
                        int socketTimeout) {
        this.voldemortConfig = config;
        String throttlerInfo = "";
        if(maxBytesPerSecond != null && maxBytesPerSecond > 0) {
            this.maxBytesPerSecond = maxBytesPerSecond;
            this.throttler = new EventThrottler(this.maxBytesPerSecond,
                                                throttlerIntervalMs,
                                                "hdfs-fetcher-node-throttler");
            throttlerInfo = "throttler with global rate = " + maxBytesPerSecond + " bytes / sec";
        } else {
            this.maxBytesPerSecond = null;
            this.throttler = null;
            throttlerInfo = "no throttler";
        }
        this.reportingIntervalBytes = Utils.notNull(reportingIntervalBytes);
        this.bufferSize = bufferSize;
        this.maxAttempts = retryCount + 1;
        this.retryDelayMs = retryDelayMs;
        this.enableStatsFile = enableStatsFile;
        this.maxVersionsStatsFile = maxVersionsStatsFile;

        logger.info("Created HdfsFetcher: " + throttlerInfo +
                ", buffer size = " + bufferSize + " bytes" +
                ", reporting interval = " + reportingIntervalBytes + " bytes" +
                ", fetcher socket timeout = " + socketTimeout + " ms.");
    }

    /**
     * Used only by unit tests and by the deprecated {@link voldemort.server.http.gui.ReadOnlyStoreManagementServlet}.
     *
     * FIXME: Refactor test code with dependency injection or scope restrictions so this function is not public.
     *
     * @deprecated Do not use for production code, use {@link #fetch(String, String, voldemort.server.protocol.admin.AsyncOperationStatus, String, long, voldemort.store.metadata.MetadataStore, Long diskQuotaSizeInKB)} instead.
     */
    @Deprecated
    @Override
    public File fetch(String source, String dest) throws Exception {
        return fetch(source, dest, VoldemortConfig.DEFAULT_DEFAULT_STORAGE_SPACE_QUOTA_IN_KB);
    }

    /**
     * Used for unit tests only.
     *
     * FIXME: Refactor test code with dependency injection or scope restrictions so this function is not public.
     *
     * @deprecated Do not use for production code, use {@link #fetch(String, String, voldemort.server.protocol.admin.AsyncOperationStatus, String, long, voldemort.store.metadata.MetadataStore, Long diskQuotaSizeInKB)} instead.
     */
    @Deprecated
    @Override
    public File fetch(String source, String dest, long diskQuotaSizeInKB) throws Exception {
        return fetchFromSource(source, dest, null, null, -1, diskQuotaSizeInKB, null);
    }

    @Override
    public File fetch(String sourceFileUrl,
                      String destinationFile,
                      AsyncOperationStatus status,
                      String storeName,
                      long pushVersion,
                      MetadataStore metadataStore,
                      Long  diskQuotaSizeInKB) throws Exception {

        logger.info("Starting fetch for : " + sourceFileUrl);
        return fetchFromSource(sourceFileUrl,
                               destinationFile,
                               status,
                               storeName,
                               pushVersion,
                               diskQuotaSizeInKB,
                               metadataStore);
    }

    private File fetchFromSource(String sourceFileUrl,
                                 String destinationFile,
                                 AsyncOperationStatus status,
                                 String storeName,
                                 long pushVersion,
                                 Long diskQuotaSizeInKB,
                                 MetadataStore metadataStore) throws Exception {
        ObjectName jmxName = null;
        HdfsCopyStats stats = null;
        FileSystem fs = null;
        sourceFileUrl = VoldemortUtils.modifyURL(sourceFileUrl, voldemortConfig);
        // Flag to indicate whether the fetch is complete or not
        boolean isCompleteFetch = false;
        try {
            // Record as one store fetch
            HdfsCopyStats.storeFetch();

            fs = HadoopUtils.getHadoopFileSystem(voldemortConfig, sourceFileUrl);
            final Path rootPath = new Path(sourceFileUrl);
            File destination = new File(destinationFile);

            if(destination.exists()) {
                throw new VoldemortException("Version directory " + destination.getAbsolutePath()
                                             + " already exists");
            }

            boolean isFile = isFile(fs, rootPath);

            stats = new HdfsCopyStats(sourceFileUrl,
                    destination,
                    false, // stats file initially disabled, to fetch just the first metadata file
                    maxVersionsStatsFile,
                    isFile,
                    null);
            jmxName = JmxUtils.registerMbean("hdfs-copy-" + copyCount.getAndIncrement(), stats);
            logger.info("Starting fetch for : " + sourceFileUrl);

            FetchStrategy fetchStrategy = new BasicFetchStrategy(this,
                                                                 fs,
                                                                 stats,
                                                                 status,
                                                                 bufferSize);
            if(!isFile) { // We are asked to fetch a directory
                Utils.mkdirs(destination);
                HdfsDirectory rootDirectory = new HdfsDirectory(fs, rootPath, this.voldemortConfig);
                List<HdfsDirectory> directoriesToFetch = Lists.newArrayList();

                HdfsFile metadataFile = rootDirectory.getMetadataFile();
                Long expectedDiskSize = -1L;

                if(metadataFile != null) {
                    File copyLocation = new File(destination, metadataFile.getPath().getName());
                    fetchStrategy.fetch(metadataFile, copyLocation, null);
                    rootDirectory.initializeMetadata(copyLocation);

                    if (metadataFile.getDiskFileName().equals(ReadOnlyUtils.FULL_STORE_METADATA_FILE)) {
                        // Then we are in build.primary.replica.only mode, and we need to determine which
                        // partition sub-directories to download
                        Set<Integer> partitions = getPartitionsForCurrentNode(metadataStore, storeName);
                        for (int partitionId: partitions) {
                            String partitionKey = ReadOnlyUtils.PARTITION_DIRECTORY_PREFIX + partitionId;
                            ReadOnlyStorageMetadata partitionMetadata = rootDirectory.getMetadata().getNestedMetadata(partitionKey);
                            String diskSizeInBytes = (String) partitionMetadata.get(ReadOnlyStorageMetadata.DISK_SIZE_IN_BYTES);
                            if (diskSizeInBytes != null && diskSizeInBytes != "") {
                                logger.debug("Partition " + partitionId + " is served by this node and is not empty, so it will be downloaded.");
                                if (expectedDiskSize == -1) {
                                    expectedDiskSize = Long.parseLong(diskSizeInBytes);
                                } else {
                                    expectedDiskSize += Long.parseLong(diskSizeInBytes);
                                }
                                HdfsDirectory partitionDirectory = new HdfsDirectory(fs,
                                                                                     new Path(rootPath, partitionKey),
                                                                                     this.voldemortConfig);
                                partitionDirectory.initializeMetadata(partitionMetadata);
                                directoriesToFetch.add(partitionDirectory);
                            } else {
                                logger.debug("Partition " + partitionId + " is served by this node but it is empty, so it will be skipped.");
                            }
                        }
                    } else {
                        // Then we are not in build.primary.replica.only mode (old behavior), and we
                        // need to download the entire node directory we're currently in.
                        String diskSizeInBytes = (String) rootDirectory.getMetadata()
                                .get(ReadOnlyStorageMetadata.DISK_SIZE_IN_BYTES);
                        if (diskSizeInBytes != null && diskSizeInBytes != "") {
                            expectedDiskSize = Long.parseLong(diskSizeInBytes);
                        }
                        directoriesToFetch.add(rootDirectory);
                    }
                }

                checkIfQuotaExceeded(diskQuotaSizeInKB,
                                     storeName,
                                     destination,
                                     expectedDiskSize);

                stats = new HdfsCopyStats(sourceFileUrl,
                        destination,
                        enableStatsFile,
                        maxVersionsStatsFile,
                        isFile,
                        new HdfsPathInfo(directoriesToFetch));

                fetchStrategy = new BasicFetchStrategy(this,
                                                       fs,
                                                       stats,
                                                       status,
                                                       bufferSize);

                logger.debug("directoriesToFetch for store '" + storeName + "': " + Arrays
                    .toString(directoriesToFetch.toArray()));
                for (HdfsDirectory directoryToFetch: directoriesToFetch) {
                    Map<HdfsFile, byte[]> fileCheckSumMap = fetchStrategy.fetch(directoryToFetch, destination);
                    if(directoryToFetch.validateCheckSum(fileCheckSumMap)) {
                        logger.info("Completed fetch: " + sourceFileUrl);
                    } else {
                        stats.checkSumFailed();
                        logger.error("Checksum did not match for " + directoryToFetch.toString() + " !");
                        return null;
                    }
                }
                isCompleteFetch = true;
                return destination;
            } else if (allowFetchingOfSingleFile) {
                /** This code path is only used by {@link #main(String[])} */
                Utils.mkdirs(destination);
                HdfsFile file = new HdfsFile(fs.getFileStatus(rootPath));
                String fileName = file.getDiskFileName();
                File copyLocation = new File(destination, fileName);
                fetchStrategy.fetch(file, copyLocation, CheckSumType.NONE);
                logger.info("Completed fetch : " + sourceFileUrl);
                isCompleteFetch = true;
                return destination;
            } else {
                logger.error("Source " + rootPath.toString() + " should be a directory");
                return null;
            }
        } catch (Exception e) {
            if(stats != null) {
                stats.reportError("File fetcher failed for destination " + destinationFile, e);
            }
            // Since AuthenticationException may happen before stats object initialization (HadoopUtils.getHadoopFileSystem),
            // we use the static method to capture all the exceptions here.
            HdfsCopyStats.reportExceptionForStats(e);

            if(e instanceof VoldemortException) {
                throw e;
            } else {
                throw new VoldemortException("Error thrown while trying to get data from Hadoop filesystem: " + e.getMessage(), e);
            }
        } finally {
            if(jmxName != null)
                JmxUtils.unregisterMbean(jmxName);

            if(stats != null) {
                stats.complete();
            }
            if (!isCompleteFetch) {
                HdfsCopyStats.incompleteFetch();
            }

            if(fs != null) {
                try {
                    fs.close();
                } catch(Exception e) {
                    String errorMessage = "Caught " + e.getClass().getSimpleName() +
                                          " while trying to close the filesystem instance (harmless).";
                    if(stats != null) {
                        stats.reportError(errorMessage, e);
                    }
                    logger.debug(errorMessage, e);
                }
            }
        }
    }

    /**
     * @return the set of partitions which need to be served (and thus fetched) by the current node.
     */
    private Set<Integer> getPartitionsForCurrentNode(MetadataStore metadataStore, String storeName) {
        Set<Integer> partitions = Sets.newHashSet();
        StoreDefinition storeDefinition = metadataStore.getStoreDef(storeName);
        Cluster cluster = metadataStore.getCluster();
        List<Integer> partitionsMasteredByCurrentNode = cluster.getNodeById(metadataStore.getNodeId()).getPartitionIds();
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition, cluster);

        // For each partition in the cluster, determine if it needs to be served by the current node
        for (int partitionId = 0; partitionId < cluster.getNumberOfPartitions(); partitionId++) {
            boolean partitionIdIsServedByCurrentNode = false;
            for (Integer replicatingPartition: routingStrategy.getReplicatingPartitionList(partitionId)) {
                if (partitionsMasteredByCurrentNode.contains(replicatingPartition)) {
                    partitionIdIsServedByCurrentNode = true;
                    break;
                }
            }
            if (partitionIdIsServedByCurrentNode) {
                partitions.add(partitionId);
            } else {
                logger.debug("Partition " + partitionId + " is not served by this node, so it will be skipped.");
            }
        }
        return partitions;
    }

    /**
     * Only check quota for those stores:that are listed in the
     * System store - voldsys$_store_quotas and have non -1 values.
     * Others are either:
     *
     * 1. already existing non quota-ed store, that will be
     * converted to quota-ed stores in future. (or)
     * 2. new stores that do not want to be affected by the
     * disk quota feature at all. -1 represents no Quota
     */
    private void checkIfQuotaExceeded(Long diskQuotaSizeInKB,
                                      String storeName,
                                      File dest,
                                      Long expectedDiskSize) {
        if(diskQuotaSizeInKB != null
                && diskQuotaSizeInKB != VoldemortConfig.DEFAULT_DEFAULT_STORAGE_SPACE_QUOTA_IN_KB) {
            String logMessage = "Store: " + storeName + ", Destination: " + dest.getAbsolutePath()
                                + ", Expected disk size in KB: "
                                + (expectedDiskSize / ByteUtils.BYTES_PER_KB)
                                + ", Disk quota size in KB: " + diskQuotaSizeInKB;
            logger.debug(logMessage);
            if(diskQuotaSizeInKB == 0L) {
                String errorMessage = "Not able to find store (" + storeName +
                        ") in this cluster according to the push URL. BnP job is not able to create new stores now." +
                        "Please reach out to a Voldemort admin if you think this is the correct cluster you want to push.";
                logger.error(errorMessage);
                throw new UnauthorizedStoreException(errorMessage);
            }
            // check if there is still sufficient quota left for this push
            Long estimatedDiskSizeNeeded = (expectedDiskSize / ByteUtils.BYTES_PER_KB);
            if(estimatedDiskSizeNeeded >= diskQuotaSizeInKB) {
                String errorMessage = "Quota Exceeded for " + logMessage;
                logger.error(errorMessage);
                throw new QuotaExceededException(errorMessage);
            }
        } else {
            logger.debug("store: " + storeName + " is a Non Quota type store.");
        }
    }

    private boolean isFile(FileSystem fs, Path rootPath) {
        for (int attempt = 1; attempt <= getMaxAttempts(); attempt++) {
            try {
                return fs.isFile(rootPath);
            } catch (IOException e) {
              logger.error("Error while calling isFile for path:" + rootPath.toString() + "  Attempt: #" + attempt + "/"
                  + getMaxAttempts());
              if (getRetryDelayMs() > 0) {
                    try {
                        Thread.sleep(getRetryDelayMs());
                    } catch (InterruptedException ie) {
                        logger.error("Fetcher is interrupted while wating to retry.", ie);
                    }

                }
            }
        }
        throw new VoldemortException(
            "After retrying " + getMaxAttempts() + "times, can not get result from filesystem for isFile.");
    }

    public Long getReportingIntervalBytes() {
        return reportingIntervalBytes;
    }

    public EventThrottler getThrottler() {
        return throttler;
    }

    public long getRetryDelayMs() {
        return retryDelayMs;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    /**
     * Main method for testing fetching
     */
    public static void main(String[] args) throws Exception {
        if(args.length < 1)
            Utils.croak("USAGE: java " + HdfsFetcher.class.getName()
                        + " url [keytab-location kerberos-username hadoop-config-path [destDir]]");
        String url = args[0];

        VoldemortConfig config = new VoldemortConfig(-1, "");

        HdfsFetcher fetcher = new HdfsFetcher(config);

        String destDir = null;
        Long diskQuotaSizeInKB;
        if(args.length >= 4) {
            fetcher.voldemortConfig.setReadOnlyKeytabPath(args[1]);
            fetcher.voldemortConfig.setReadOnlyKerberosUser(args[2]);
            fetcher.voldemortConfig.setHadoopConfigPath(args[3]);
        }
        if(args.length >= 5)
            destDir = args[4];

        if(args.length >= 6)
            diskQuotaSizeInKB = Long.parseLong(args[5]);
        else
            diskQuotaSizeInKB = null;

        // for testing we want to be able to download a single file
        allowFetchingOfSingleFile = true;

        FileSystem fs = HadoopUtils.getHadoopFileSystem(fetcher.voldemortConfig, url);
        Path p = new Path(url);

        FileStatus status = fs.listStatus(p)[0];
        long size = status.getLen();
        long start = System.currentTimeMillis();
        if(destDir == null)
            destDir = System.getProperty("java.io.tmpdir") + File.separator + start;

        File location = fetcher.fetch(url, destDir, null, null, -1, null, diskQuotaSizeInKB);

        double rate = size * Time.MS_PER_SECOND / (double) (System.currentTimeMillis() - start);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(2);
        System.out.println("Fetch to " + location + " completed: "
                           + nf.format(rate / (1024.0 * 1024.0)) + " MB/sec.");
        fs.close();
    }
}
