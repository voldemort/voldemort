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

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AdminServiceRequestHandler;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.quota.QuotaType;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.store.readonly.mr.utils.VoldemortUtils;
import voldemort.store.readonly.swapper.InvalidBootstrapURLException;
import voldemort.utils.ByteUtils;
import voldemort.utils.EventThrottler;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

/**
 * A {@link FileFetcher} implementation that fetches the store files from HDFS
 */
public class HdfsFetcher implements FileFetcher {

    // Class-level state
    private static final Logger logger = Logger.getLogger(HdfsFetcher.class);
    private static final AtomicInteger copyCount = new AtomicInteger(0);
    private static Boolean allowFetchOfFiles = false;

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
             VoldemortConfig.REPORTING_INTERVAL_BYTES,
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
     * @deprecated Do not use for production code, use {@link #fetch(String, String, voldemort.server.protocol.admin.AsyncOperationStatus, String, long, voldemort.store.metadata.MetadataStore)} instead.
     */
    @Deprecated
    @Override
    public File fetch(String source, String dest) throws Exception {
        return fetch(source, dest, VoldemortConfig.DEFAULT_STORAGE_SPACE_QUOTA_IN_KB);
    }

    /**
     * Used for unit tests only.
     *
     * FIXME: Refactor test code with dependency injection or scope restrictions so this function is not public.
     *
     * @deprecated Do not use for production code, use {@link #fetch(String, String, voldemort.server.protocol.admin.AsyncOperationStatus, String, long, voldemort.store.metadata.MetadataStore)} instead.
     */
    @Deprecated
    @Override
    public File fetch(String source, String dest, long diskQuotaSizeInKB) throws Exception {
        return fetchFromSource(source, dest, null, null, -1, diskQuotaSizeInKB);
    }

    @Override
    public File fetch(String sourceFileUrl,
                      String destinationFile,
                      AsyncOperationStatus status,
                      String storeName,
                      long pushVersion,
                      MetadataStore metadataStore) throws Exception {
        AdminClient adminClient = null;
        try {
            adminClient = new AdminClient(metadataStore.getCluster(),
                                          new AdminClientConfig(),
                                          new ClientConfig());

            Versioned<String> diskQuotaSize = adminClient.quotaMgmtOps.getQuotaForNode(storeName,
                                                                                       QuotaType.STORAGE_SPACE,
                                                                                       metadataStore.getNodeId());
            Long diskQuoataSizeInKB = (diskQuotaSize == null) ? null : (Long.parseLong(diskQuotaSize.getValue()));
            logger.info("Starting fetch for : " + sourceFileUrl);
            return fetchFromSource(sourceFileUrl,
                                   destinationFile,
                                   status,
                                   storeName,
                                   pushVersion,
                                   diskQuoataSizeInKB);
        } finally {
            if(adminClient != null) {
                IOUtils.closeQuietly(adminClient);
            }
        }
    }

    private File fetchFromSource(String sourceFileUrl,
                          String destinationFile,
                          AsyncOperationStatus status,
                          String storeName,
                          long pushVersion,
                          Long diskQuotaSizeInKB) throws Exception {
        ObjectName jmxName = null;
        HdfsCopyStats stats = null;
        FileSystem fs = null;
        sourceFileUrl = VoldemortUtils
            .modifyURL(sourceFileUrl, voldemortConfig.getModifiedProtocol(), voldemortConfig.getModifiedPort());
        try {
            fs = HadoopUtils.getHadoopFileSystem(voldemortConfig, sourceFileUrl);
            final Path path = new Path(sourceFileUrl);
            File destination = new File(destinationFile);

            if(destination.exists()) {
                throw new VoldemortException("Version directory " + destination.getAbsolutePath()
                                             + " already exists");
            }

            boolean isFile = fs.isFile(path);

            stats = new HdfsCopyStats(sourceFileUrl,
                                      destination,
                                      enableStatsFile,
                                      maxVersionsStatsFile,
                                      isFile,
                                      new HdfsPathInfo(fs, path));
            jmxName = JmxUtils.registerMbean("hdfs-copy-" + copyCount.getAndIncrement(), stats);
            logger.info("Starting fetch for : " + sourceFileUrl);

            FetchStrategy fetchStrategy = new BasicFetchStrategy(this,
                                                                 fs,
                                                                 stats,
                                                                 status,
                                                                 bufferSize);
            if(!fs.isFile(path)) {
                Utils.mkdirs(destination);
                HdfsDirectory directory = new HdfsDirectory(fs, path);

                HdfsFile metadataFile = directory.getMetadataFile();
                Long estimatedDiskSize = -1L;

                if(metadataFile != null) {
                    File copyLocation = new File(destination, metadataFile.getPath().getName());
                    fetchStrategy.fetch(metadataFile, copyLocation, null);
                    directory.initializeMetadata(copyLocation);
                    String diskSizeInBytes = (String) directory.getMetadata()
                                                               .get(ReadOnlyStorageMetadata.DISK_SIZE_IN_BYTES);
                    estimatedDiskSize = (diskSizeInBytes != null && diskSizeInBytes != "") ? (Long.parseLong(diskSizeInBytes))
                                                                                          : -1L;
                }


                /*
                 * Only check quota for those stores:that are listed in the
                 * System store - voldsys$_store_quotas and have non -1 values.
                 * Others are either:
                 * 
                 * 1. already existing non quota-ed store, that will be
                 * converted to quota-ed stores in future. (or) 2. new stores
                 * that do not want to be affected by the disk quota feature at
                 * all. -1 represents no Quota
                 */

                if(diskQuotaSizeInKB != null
                   && diskQuotaSizeInKB != VoldemortConfig.DEFAULT_STORAGE_SPACE_QUOTA_IN_KB) {
                    checkIfQuotaExceeded(diskQuotaSizeInKB,
                                         storeName,
                                         destination,
                                         estimatedDiskSize);
                } else {
                    if(logger.isDebugEnabled()) {
                        logger.debug("store: " + storeName + " is a Non Quota type store.");
                    }
                }
                Map<HdfsFile, byte[]> fileCheckSumMap = fetchStrategy.fetch(directory, destination);
                if(directory.validateCheckSum(fileCheckSumMap)) {
                    logger.info("Completed fetch : " + sourceFileUrl);
                    return destination;
                } else {
                    logger.error("Checksum did not match!");
                    return null;
                }
            } else if(allowFetchOfFiles) {
                Utils.mkdirs(destination);
                HdfsFile file = new HdfsFile(fs.getFileStatus(path));
                String fileName = file.getDiskFileName();
                File copyLocation = new File(destination, fileName);
                fetchStrategy.fetch(file, copyLocation, CheckSumType.NONE);
                logger.info("Completed fetch : " + sourceFileUrl);
                return destination;
            } else {
                logger.error("Source " + path.toString() + " should be a directory");
                return null;
            }
        } catch (Exception e) {
            if(stats != null) {
                stats.reportError("File fetcher failed for destination " + destinationFile, e);
            }
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

            if(fs != null) {
                try {
                    fs.close();
                } catch(IOException e) {
                    String errorMessage = "Got IOException while trying to close the filesystem instance (harmless).";
                    if(stats != null) {
                        stats.reportError(errorMessage, e);
                    }
                    logger.info(errorMessage, e);
                }
            }
        }
    }

    private void checkIfQuotaExceeded(Long diskQuotaSizeInKB,
                            String storeName,
                            File dest,
                            Long estimatedDiskSize) {
        String logMessage = "Store: " + storeName + ", Destination: " + dest.getAbsolutePath()
                            + ", Expected disk size in KB: "
                            + (estimatedDiskSize / ByteUtils.BYTES_PER_KB)
                            + ", Disk quota size in KB: " + diskQuotaSizeInKB;
        if(logger.isDebugEnabled()) {
            logger.error(logMessage);
        }
        if(diskQuotaSizeInKB == 0L) {
            String errorMessage = "This store: \'"
                                  + storeName
                                  + "\' does not belong to this Voldemort cluster. Please use a valid bootstrap url.";
            logger.error(errorMessage);
            throw new InvalidBootstrapURLException(errorMessage);
        }
        // check if there is still sufficient quota left for this push
        Long estimatedDiskSizeNeeded = (estimatedDiskSize / ByteUtils.BYTES_PER_KB);
        if(estimatedDiskSizeNeeded >= diskQuotaSizeInKB) {
            String errorMessage = "Quota Exceeded for " + logMessage;
            logger.error(errorMessage);
            throw new QuotaExceededException(errorMessage);
        }
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

        HdfsFetcher fetcher = new HdfsFetcher();

        String destDir = null;
        if(args.length >= 4) {
            fetcher.voldemortConfig.setReadOnlyKeytabPath(args[1]);
            fetcher.voldemortConfig.setReadOnlyKerberosUser(args[2]);
            fetcher.voldemortConfig.setHadoopConfigPath(args[3]);
        }
        if(args.length >= 5)
            destDir = args[4];

        // for testing we want to be able to download a single file
        allowFetchOfFiles = true;

        FileSystem fs = HadoopUtils.getHadoopFileSystem(fetcher.voldemortConfig, url);
        Path p = new Path(url);

        FileStatus status = fs.listStatus(p)[0];
        long size = status.getLen();
        long start = System.currentTimeMillis();
        if(destDir == null)
            destDir = System.getProperty("java.io.tmpdir") + File.separator + start;

        File location = fetcher.fetch(url, destDir, null, null, -1, null);

        double rate = size * Time.MS_PER_SECOND / (double) (System.currentTimeMillis() - start);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(2);
        System.out.println("Fetch to " + location + " completed: "
                           + nf.format(rate / (1024.0 * 1024.0)) + " MB/sec.");
        fs.close();
    }
}
