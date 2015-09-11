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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AdminServiceRequestHandler;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.utils.EventThrottler;
import voldemort.utils.ExceptionUtils;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * A fetcher that fetches the store files from HDFS
 */
public class HdfsFetcher implements FileFetcher {

    private static final Logger logger = Logger.getLogger(HdfsFetcher.class);

    public static final String GZIP_FILE_EXTENSION = ".gz";
    public static final String INDEX_FILE_EXTENSION = ".index";
    public static final String DATA_FILE_EXTENSION = ".data";
    public static final String METADATA_FILE_EXTENSION = ".metadata";
    private final Long maxBytesPerSecond, reportingIntervalBytes;
    private final int bufferSize;
    private static final AtomicInteger copyCount = new AtomicInteger(0);
    private EventThrottler throttler = null;
    private long retryDelayMs = 0;
    private int maxAttempts = 0;
    private VoldemortConfig voldemortConfig = null;
    private final boolean enableStatsFile;
    private final int maxVersionsStatsFile;

    private static Boolean allowFetchOfFiles = false;

    /**
     * this is the constructor invoked via reflection from
     * {@link AdminServiceRequestHandler#setFetcherClass(voldemort.server.VoldemortConfig)}
     */
    public HdfsFetcher(VoldemortConfig config) {
        this(config.getReadOnlyFetcherMaxBytesPerSecond(),
             config.getReadOnlyFetcherReportingIntervalBytes(),
             config.getReadOnlyFetcherThrottlerInterval(),
             config.getFetcherBufferSize(),
             config.getReadOnlyFetchRetryCount(),
             config.getReadOnlyFetchRetryDelayMs(),
             config.isReadOnlyStatsFileEnabled(),
             config.getReadOnlyMaxVersionsStatsFile(),
             config.getFetcherSocketTimeout());
        this.voldemortConfig = config;
    }

    // Test-only constructor
    public HdfsFetcher() {
        this((Long) null,
             VoldemortConfig.REPORTING_INTERVAL_BYTES,
             VoldemortConfig.DEFAULT_FETCHER_THROTTLE_INTERVAL_WINDOW_MS,
             VoldemortConfig.DEFAULT_FETCHER_BUFFER_SIZE,
             3,
             1000,
             true,
             50,
             VoldemortConfig.DEFAULT_FETCHER_SOCKET_TIMEOUT);
    }

    public HdfsFetcher(Long maxBytesPerSecond,
                       Long reportingIntervalBytes,
                       int throttlerIntervalMs,
                       int bufferSize,
                       int retryCount,
                       long retryDelayMs,
                       boolean enableStatsFile,
                       int maxVersionsStatsFile,
                       int socketTimeout) {
        String throttlerInfo = "";
        if(maxBytesPerSecond != null && maxBytesPerSecond > 0) {
            this.maxBytesPerSecond = maxBytesPerSecond;
            this.throttler = new EventThrottler(this.maxBytesPerSecond,
                                                throttlerIntervalMs,
                                                "hdfs-fetcher-node-throttler");
            throttlerInfo = "throttler with global rate = " + maxBytesPerSecond + " bytes / sec";
        } else {
            this.maxBytesPerSecond = null;
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

    public File fetch(String source, String dest) throws IOException {
        return fetch(source, dest, null, null, -1, null);
    }

    @Override
    public File fetch(String source,
                      String dest,
                      AsyncOperationStatus status,
                      String storeName,
                      long pushVersion,
                      MetadataStore metadataStore) throws IOException {
        String hadoopConfigPath = "";
        if (this.voldemortConfig != null) {
            hadoopConfigPath = this.voldemortConfig.getHadoopConfigPath();
        }
        return fetch(source,
                     dest,
                     status,
                     storeName,
                     pushVersion,
                     metadataStore,
                     hadoopConfigPath);
    }

    public File fetch(String sourceFileUrl,
                      String destinationFile,
                      AsyncOperationStatus status,
                      String storeName,
                      long pushVersion,
                      MetadataStore metadataStore,
                      String hadoopConfigPath) throws IOException {

        ObjectName jmxName = null;
        HdfsCopyStats stats = null;
        FileSystem fs = null;
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
            boolean result =
                    fetch(fs,
                          path,
                          destination,
                          status,
                          stats,
                          storeName,
                          pushVersion,
                          metadataStore);
            logger.info("Completed fetch : " + sourceFileUrl);

            if(result) {
                return destination;
            } else {
                return null;
            }
        } catch (Exception e) {
            if(stats != null) {
                stats.reportError("File fetcher failed for destination " + destinationFile, e);
            }
            throw new VoldemortException("Error thrown while trying to get data from Hadoop filesystem : ", e);

        } finally {
            if(jmxName != null)
                JmxUtils.unregisterMbean(jmxName);

            if(stats != null) {
                stats.complete();
            }

            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    String errorMessage = "Got IOException while trying to close the filesystem instance (harmless).";
                    if(stats != null) {
                        stats.reportError(errorMessage, e);
                    }
                    logger.info(errorMessage, e);
                }
            }
        }
    }

    private boolean fetch(FileSystem fs,
                          Path source,
                          File dest,
                          AsyncOperationStatus status,
                          HdfsCopyStats stats,
                          String storeName,
                          long pushVersion,
                          MetadataStore metadataStore) throws IOException {
        FetchStrategy fetchStrategy =
                new BasicFetchStrategy(this, fs, stats, status, bufferSize);
        if (!fs.isFile(source)) {
            Utils.mkdirs(dest);
            HdfsDirectory directory = new HdfsDirectory(fs, source);

            HdfsFile metadataFile = directory.getMetadataFile();

            if (metadataFile != null) {
                File copyLocation =
                        new File(dest, metadataFile.getPath().getName());
                fetchStrategy.fetch(metadataFile, copyLocation, null);
                directory.initializeMetadata(copyLocation);
            }

            Map<HdfsFile, byte[]> fileCheckSumMap =
                    fetchStrategy.fetch(directory, dest);

            return directory.validateCheckSum(fileCheckSumMap);

        } else if (allowFetchOfFiles) {
            Utils.mkdirs(dest);
            HdfsFile file = new HdfsFile(fs.getFileStatus(source));
            String fileName = file.getDiskFileName();
            File copyLocation = new File(dest, fileName);
            fetchStrategy.fetch(file, copyLocation, CheckSumType.NONE);
            return true;
        }
        logger.error("Source " + source.toString() + " should be a directory");
        return false;
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

    /*
     * Main method for testing fetching
     */
    public static void main(String[] args) throws Exception {
        if(args.length < 1)
            Utils.croak("USAGE: java " + HdfsFetcher.class.getName()
                        + " url [keytab-location kerberos-username hadoop-config-path [destDir]]");
        String url = args[0];

        String keytabLocation = "";
        String kerberosUser = "";
        String hadoopPath = "";
        String destDir = null;
        if(args.length >= 4) {
            keytabLocation = args[1];
            kerberosUser = args[2];
            hadoopPath = args[3];
        }
        if(args.length >= 5)
            destDir = args[4];

        // for testing we want to be able to download a single file
        allowFetchOfFiles = true;

        long maxBytesPerSec = 1024 * 1024 * 1024;

        final Configuration config = new Configuration();
        config.setInt("io.file.buffer.size", VoldemortConfig.DEFAULT_FETCHER_BUFFER_SIZE);
        config.set("hadoop.rpc.socket.factory.class.ClientProtocol",
                   ConfigurableSocketFactory.class.getName());
        config.setInt("io.socket.receive.buffer", 1 * 1024 * 1024 - 10000);

        FileSystem fs = null;
        Path p = new Path(url);

        boolean isHftpBasedFetch = url.length() > 4 && url.substring(0, 4).equals("hftp");
        logger.info("URL : " + url + " and hftp protocol enabled = " + isHftpBasedFetch);

        if(hadoopPath.length() > 0 && !isHftpBasedFetch) {
            config.set("hadoop.security.group.mapping",
                       "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");

            config.addResource(new Path(hadoopPath + "/core-site.xml"));
            config.addResource(new Path(hadoopPath + "/hdfs-site.xml"));

            String security = config.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);

            if(security == null || !security.equals("kerberos")) {
                logger.info("Security isn't turned on in the conf: "
                            + CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION + " = "
                            + config.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION));
                logger.info("Fix that.  Exiting.");
                return;
            } else {
                logger.info("Security is turned on in the conf. Trying to authenticate ...");
            }
        }

        try {

            // Get the filesystem object
            if(keytabLocation.length() > 0 && !isHftpBasedFetch) {
                UserGroupInformation.setConfiguration(config);
                UserGroupInformation.loginUserFromKeytab(kerberosUser, keytabLocation);

                final Path path = p;
                try {
                    logger.debug("I've logged in and am now Doasing as "
                                 + UserGroupInformation.getCurrentUser().getUserName());
                    fs = UserGroupInformation.getCurrentUser()
                                             .doAs(new PrivilegedExceptionAction<FileSystem>() {

                                                 public FileSystem run() throws Exception {
                                                     FileSystem fs = path.getFileSystem(config);
                                                     return fs;
                                                 }
                                             });
                } catch(InterruptedException e) {
                    logger.error(e.getMessage());
                } catch(Exception e) {
                    logger.error("Got an exception while getting the filesystem object: ");
                    logger.error("Exception class : " + e.getClass());
                    e.printStackTrace();
                    for(StackTraceElement et: e.getStackTrace()) {
                        logger.error(et.toString());
                    }
                }
            } else {
                fs = p.getFileSystem(config);
            }

        } catch(IOException e) {
            e.printStackTrace();
            System.err.println("IOException in getting Hadoop filesystem object !!! Exiting !!!");
            System.exit(-1);
        } catch(Throwable te) {
            te.printStackTrace();
            logger.error("Error thrown while trying to get Hadoop filesystem");
            System.exit(-1);
        }

        FileStatus status = fs.listStatus(p)[0];
        long size = status.getLen();
        HdfsFetcher fetcher = new HdfsFetcher(maxBytesPerSec,
                                              VoldemortConfig.REPORTING_INTERVAL_BYTES,
                                              VoldemortConfig.DEFAULT_FETCHER_THROTTLE_INTERVAL_WINDOW_MS,
                                              VoldemortConfig.DEFAULT_FETCHER_BUFFER_SIZE,
                                              5,
                                              5000,
                                              true,
                                              50,
                                              VoldemortConfig.DEFAULT_FETCHER_SOCKET_TIMEOUT);
        long start = System.currentTimeMillis();
        if(destDir == null)
            destDir = System.getProperty("java.io.tmpdir") + File.separator + start;

        File location = fetcher.fetch(url, destDir, null, null, -1, null, hadoopPath);

        double rate = size * Time.MS_PER_SECOND / (double) (System.currentTimeMillis() - start);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(2);
        System.out.println("Fetch to " + location + " completed: "
                           + nf.format(rate / (1024.0 * 1024.0)) + " MB/sec.");
        fs.close();
    }
}
