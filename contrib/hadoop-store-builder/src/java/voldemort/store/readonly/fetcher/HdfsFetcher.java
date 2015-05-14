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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import javax.management.ObjectName;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AdminServiceRequestHandler;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.DynamicEventThrottler;
import voldemort.utils.DynamicThrottleLimit;
import voldemort.utils.EventThrottler;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/*
 * A fetcher that fetches the store files from HDFS
 */
public class HdfsFetcher implements FileFetcher {

    private static final Logger logger = Logger.getLogger(HdfsFetcher.class);

    private static String keytabPath = "";
    private static String kerberosPrincipal = VoldemortConfig.DEFAULT_KERBEROS_PRINCIPAL;
    public static final String GZIP_FILE_EXTENSION = ".gz";
    public static final String INDEX_FILE_EXTENSION = ".index";
    public static final String DATA_FILE_EXTENSION = ".data";
    public static final String METADATA_FILE_EXTENSION = ".metadata";
    private static String COMPRESSED_INDEX_FILE_EXTENSION = INDEX_FILE_EXTENSION
                                                            + GZIP_FILE_EXTENSION;
    private static String COMPRESSED_DATA_FILE_EXTENSION = DATA_FILE_EXTENSION
                                                           + GZIP_FILE_EXTENSION;

    public static final String[] validExtensions = { INDEX_FILE_EXTENSION,
            COMPRESSED_INDEX_FILE_EXTENSION, DATA_FILE_EXTENSION, COMPRESSED_DATA_FILE_EXTENSION };

    private final Long maxBytesPerSecond, reportingIntervalBytes;
    private final int bufferSize;
    private static final AtomicInteger copyCount = new AtomicInteger(0);
    private AsyncOperationStatus status;
    private EventThrottler throttler = null;
    private long minBytesPerSecond = 0;
    private long retryDelayMs = 0;
    private int maxAttempts = 0;
    private DynamicThrottleLimit globalThrottleLimit = null;
    private VoldemortConfig voldemortConfig = null;
    private final boolean enableStatsFile;
    private final int maxVersionsStatsFile;

    public static final String FS_DEFAULT_NAME = "fs.default.name";

    private static Boolean allowFetchOfFiles = false;

    /* Additional constructor invoked from ReadOnlyStoreManagementServlet */
    public HdfsFetcher(VoldemortConfig config) {
        this(config, null);
    }

    /**
     * this is the actual constructor invoked from
     * {@link AdminServiceRequestHandler} via reflection
     */
    public HdfsFetcher(VoldemortConfig config, DynamicThrottleLimit dynThrottleLimit) {
        this(dynThrottleLimit,
             null,
             config.getReadOnlyFetcherReportingIntervalBytes(),
             config.getFetcherBufferSize(),
             config.getReadOnlyFetcherMinBytesPerSecond(),
             config.getReadOnlyKeytabPath(),
             config.getReadOnlyKerberosUser(),
             config.getReadOnlyFetchRetryCount(),
             config.getReadOnlyFetchRetryDelayMs(),
             config.isReadOnlyStatsFileEnabled(),
             config.getReadOnlyMaxVersionsStatsFile());
        this.voldemortConfig = config;

        if(dynThrottleLimit == null) {
            logger.info("Created hdfs fetcher with no dynamic throttler, buffer size " + bufferSize
                        + ", reporting interval bytes " + reportingIntervalBytes);
        } else {
            logger.info("Created hdfs fetcher with throttle rate " + dynThrottleLimit.getRate()
                        + ", buffer size " + bufferSize + ", reporting interval bytes "
                        + reportingIntervalBytes);
        }
    }

    // Test-only constructor
    public HdfsFetcher() {
        this((Long) null,
             VoldemortConfig.REPORTING_INTERVAL_BYTES,
             VoldemortConfig.DEFAULT_BUFFER_SIZE);
    }

    // Test-only constructor
    public HdfsFetcher(Long maxBytesPerSecond, Long reportingIntervalBytes, int bufferSize) {
        this(null,
             maxBytesPerSecond,
             reportingIntervalBytes,
             bufferSize,
             0,
             "",
             "",
             3,
             1000,
             true,
             50);
    }

    public HdfsFetcher(DynamicThrottleLimit dynThrottleLimit,
                       Long maxBytesPerSecond,
                       Long reportingIntervalBytes,
                       int bufferSize,
                       long minBytesPerSecond,
                       String keytabLocation,
                       String kerberosUser,
                       int retryCount,
                       long retryDelayMs,
                       boolean enableStatsFile,
                       int maxVersionsStatsFile) {
        if(maxBytesPerSecond != null) {
            this.maxBytesPerSecond = maxBytesPerSecond;
            this.throttler = new EventThrottler(this.maxBytesPerSecond);
        } else if(dynThrottleLimit != null && dynThrottleLimit.getRate() != 0) {
            this.maxBytesPerSecond = dynThrottleLimit.getRate();
            this.throttler = new DynamicEventThrottler(dynThrottleLimit);
            this.globalThrottleLimit = dynThrottleLimit;
            logger.info("Initializing Dynamic Event throttler with rate : "
                        + this.maxBytesPerSecond + " bytes / sec");
        } else
            this.maxBytesPerSecond = null;
        this.reportingIntervalBytes = Utils.notNull(reportingIntervalBytes);
        this.bufferSize = bufferSize;
        this.status = null;
        this.minBytesPerSecond = minBytesPerSecond;
        this.maxAttempts = retryCount + 1;
        this.retryDelayMs = retryDelayMs;
        this.enableStatsFile = enableStatsFile;
        this.maxVersionsStatsFile = maxVersionsStatsFile;
        HdfsFetcher.kerberosPrincipal = kerberosUser;
        HdfsFetcher.keytabPath = keytabLocation;
    }

    public File fetch(String sourceFileUrl, String destinationFile) throws IOException {
        String hadoopConfigPath = "";
        if(this.voldemortConfig != null) {
            hadoopConfigPath = this.voldemortConfig.getHadoopConfigPath();
        }
        return fetch(sourceFileUrl, destinationFile, hadoopConfigPath);
    }

    private static boolean isHftpBasedPath(String sourceFileUrl) {
        return sourceFileUrl.length() > 4 && sourceFileUrl.substring(0, 4).equals("hftp");
    }

    private Configuration getConfiguration(String sourceFileUrl, String hadoopConfigPath) {
        final Configuration config = new Configuration();
        config.setInt("io.socket.receive.buffer", bufferSize);
        config.set("hadoop.rpc.socket.factory.class.ClientProtocol",
                   ConfigurableSocketFactory.class.getName());
        config.set("hadoop.security.group.mapping",
                   "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");

        boolean isHftpBasedFetch = isHftpBasedPath(sourceFileUrl);
        logger.info("URL : " + sourceFileUrl + " and hftp protocol enabled = " + isHftpBasedFetch);
        logger.info("Hadoop path = " + hadoopConfigPath + " , keytab path = "
                    + HdfsFetcher.keytabPath + " , kerberos principal = "
                    + HdfsFetcher.kerberosPrincipal);

        if(hadoopConfigPath.length() > 0 && !isHftpBasedFetch) {

            config.addResource(new Path(hadoopConfigPath + "/core-site.xml"));
            config.addResource(new Path(hadoopConfigPath + "/hdfs-site.xml"));

            String security = config.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);

            if(security == null || !security.equals("kerberos")) {
                logger.error("Security isn't turned on in the conf: "
                             + CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION + " = "
                             + config.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION));
                logger.error("Please make sure that the Hadoop config directory path is valid.");
                throw new VoldemortException("Error in getting Hadoop filesystem. Invalid Hadoop config directory path.");
            } else {
                logger.info("Security is turned on in the conf.");
            }
        }
        return config;
    }

    private FileSystem getHadoopFileSystem(String sourceFileUrl, String hadoopConfigPath)
            throws IOException {
        final Configuration config = getConfiguration(sourceFileUrl, hadoopConfigPath);
        final Path path = new Path(sourceFileUrl);
        boolean isHftpBasedFetch = isHftpBasedPath(sourceFileUrl);
        FileSystem fs = null;

        if(HdfsFetcher.keytabPath.length() > 0 && !isHftpBasedFetch) {

            /*
             * We're seeing intermittent errors while trying to get the Hadoop
             * filesystem in a privileged doAs block. This happens when we fetch
             * the files over hdfs or webhdfs. This retry loop is inserted here
             * as a temporary measure.
             */
            for(int attempt = 0; attempt < maxAttempts; attempt++) {
                boolean isValidFilesystem = false;

                if(!new File(HdfsFetcher.keytabPath).exists()) {
                    logger.error("Invalid keytab file path. Please provide a valid keytab path");
                    throw new VoldemortException("Error in getting Hadoop filesystem. Invalid keytab file path.");
                }

                /*
                 * The Hadoop path for getting a Filesystem object in a
                 * privileged doAs block is not thread safe. This might be
                 * causing intermittent NPE exceptions. Adding a synchronized
                 * block.
                 */
                synchronized(this) {
                    /*
                     * First login using the specified principal and keytab file
                     */
                    UserGroupInformation.setConfiguration(config);
                    UserGroupInformation.loginUserFromKeytab(HdfsFetcher.kerberosPrincipal,
                                                             HdfsFetcher.keytabPath);

                    /*
                     * If login is successful, get the filesystem object. NOTE:
                     * Ideally we do not need a doAs block for this. Consider
                     * removing it in the future once the Hadoop jars have the
                     * corresponding patch (tracked in the Hadoop Apache
                     * project: HDFS-3367)
                     */
                    try {
                        logger.info("I've logged in and am now Doasing as "
                                    + UserGroupInformation.getCurrentUser().getUserName());
                        fs = UserGroupInformation.getCurrentUser()
                                                 .doAs(new PrivilegedExceptionAction<FileSystem>() {

                                                     @Override
                                                     public FileSystem run() throws Exception {
                                                         FileSystem fs = path.getFileSystem(config);
                                                         return fs;
                                                     }
                                                 });
                        isValidFilesystem = true;
                    } catch(InterruptedException e) {
                        logger.error(e.getMessage(), e);
                    } catch(Exception e) {
                        logger.error("Got an exception while getting the filesystem object: ");
                        logger.error("Exception class : " + e.getClass());
                        e.printStackTrace();
                        for(StackTraceElement et: e.getStackTrace()) {
                            logger.error(et.toString());
                        }
                    }
                }

                if(isValidFilesystem) {
                    break;
                } else if(attempt < maxAttempts - 1) {
                    logger.error("Attempt#" + attempt
                                 + " Could not get a valid Filesystem object. Trying again in "
                                 + retryDelayMs + " ms");
                    sleepForRetryDelayMs();
                }
            }
        } else {
            fs = path.getFileSystem(config);
        }
        return fs;
    }

    public File fetch(String sourceFileUrl, String destinationFile, String hadoopConfigPath)
            throws IOException {
        if(this.globalThrottleLimit != null) {
            if(this.globalThrottleLimit.getSpeculativeRate() < this.minBytesPerSecond)
                throw new VoldemortException("Too many push jobs.");
            this.globalThrottleLimit.incrementNumJobs();
        }

        ObjectName jmxName = null;
        HdfsCopyStats stats = null;
        try {

            final FileSystem fs = getHadoopFileSystem(sourceFileUrl, hadoopConfigPath);
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
                                      sizeOfPath(fs, path));
            jmxName = JmxUtils.registerMbean("hdfs-copy-" + copyCount.getAndIncrement(), stats);



            logger.info("Starting fetch for : " + sourceFileUrl);
            boolean result = fetch(fs, path, destination, stats);
            logger.info("Completed fetch : " + sourceFileUrl);

            // Close the filesystem
            fs.close();

            if(result) {
                return destination;
            } else {
                return null;
            }
        } catch(Throwable te) {
            logger.error("Error thrown while trying to get data from Hadoop filesystem", te);
            if(stats != null) {
                stats.reportError("File fetcher failed for destination " + destinationFile, te);
            }
            throw new VoldemortException("Error thrown while trying to get data from Hadoop filesystem : "
                                         + te);

        } finally {
            if(this.globalThrottleLimit != null) {
                this.globalThrottleLimit.decrementNumJobs();
            }
            if(jmxName != null)
                JmxUtils.unregisterMbean(jmxName);

            if(stats != null) {
                stats.complete();
            }
        }
    }

    private void sleepForRetryDelayMs() {
        if(retryDelayMs > 0) {
            try {
                Thread.sleep(retryDelayMs);
            } catch(InterruptedException ie) {
                logger.error("Fetcher interrupted while waiting to retry", ie);
            }
        }
    }

    private boolean fetch(FileSystem fs, Path source, File dest, HdfsCopyStats stats)
            throws Throwable {
        if(!fs.isFile(source)) {
            Utils.mkdirs(dest);
            HdfsDirectory directory = new HdfsDirectory(fs, source);
            byte[] buffer = new byte[bufferSize];
            HdfsFile metadataFile = directory.getMetadataFile();

            if(metadataFile != null) {
                File copyLocation = new File(dest, metadataFile.getPath().getName());
                copyFileWithCheckSum(fs, metadataFile, copyLocation, stats, null, buffer);
                directory.initializeMetadata(copyLocation);
            }

            Map<HdfsFile, byte[]> fileCheckSumMap = new HashMap<HdfsFile, byte[]>(directory.getFiles()
                                                                                           .size());
            CheckSumType checkSumType = directory.getCheckSumType();
            for(HdfsFile file: directory.getFiles()) {
                String fileName = file.getDiskFileName();
                File copyLocation = new File(dest, fileName);
                CheckSum fileCheckSumGenerator = copyFileWithCheckSum(fs,
                                                                      file,
                                                                      copyLocation,
                                                                      stats,
                                                                      checkSumType,
                                                                      buffer);
                if(fileCheckSumGenerator != null) {
                    fileCheckSumMap.put(file, fileCheckSumGenerator.getCheckSum());
                }
            }

            return directory.validateCheckSum(fileCheckSumMap);

        } else if(allowFetchOfFiles) {
            Utils.mkdirs(dest);
            byte[] buffer = new byte[bufferSize];
            HdfsFile file = new HdfsFile(fs.getFileStatus(source));
            String fileName = file.getDiskFileName();
            File copyLocation = new File(dest, fileName);
            copyFileWithCheckSum(fs, file, copyLocation, stats, CheckSumType.NONE, buffer);
            return true;
        }
        logger.error("Source " + source.toString() + " should be a directory");
        return false;
    }

    // Used by tests
    private CheckSum copyFileWithCheckSumTest(FileSystem fs,
                                              Path source,
                                              File dest,
                                              HdfsCopyStats stats,
                                              CheckSumType checkSumType,
                                              byte[] buffer) throws Throwable {
        return copyFileWithCheckSum(fs,
                                    new HdfsFile(fs.getFileStatus(source)),
                                    dest,
                                    stats,
                                    checkSumType,
                                    buffer);
    }

    /**
     * Function to copy a file from the given filesystem with a checksum of type
     * 'checkSumType' computed and returned. In case an error occurs during such
     * a copy, we do a retry for a maximum of NUM_RETRIES
     * 
     * @param fs Filesystem used to copy the file
     * @param source Source path of the file to copy
     * @param dest Destination path of the file on the local machine
     * @param stats Stats for measuring the transfer progress
     * @param checkSumType Type of the Checksum to be computed for this file
     * @return A Checksum (generator) of type checkSumType which contains the
     *         computed checksum of the copied file
     * @throws IOException
     */
    private CheckSum copyFileWithCheckSum(FileSystem fs,
                                          HdfsFile source,
                                          File dest,
                                          HdfsCopyStats stats,
                                          CheckSumType checkSumType,
                                          byte[] buffer) throws Throwable {
        CheckSum fileCheckSumGenerator = null;
        logger.debug("Starting copy of " + source + " to " + dest);

        // Check if its Gzip compressed
        boolean isCompressed = source.isCompressed();
        FilterInputStream input = null;

        OutputStream output = null;
        long startTimeMS = System.currentTimeMillis();

        for(int attempt = 0; attempt < maxAttempts; attempt++) {
            boolean success = true;
            long totalBytesRead = 0;
            boolean fsOpened = false;
            try {

                // Create a per file checksum generator
                if(checkSumType != null) {
                    fileCheckSumGenerator = CheckSum.getInstance(checkSumType);
                }

                logger.info("Attempt " + attempt + " at copy of " + source + " to " + dest);

                if(!isCompressed) {
                    input = fs.open(source.getPath());
                } else {
                    // We are already bounded by the "hdfs.fetcher.buffer.size"
                    // specified in the Voldemort config, the default value of
                    // which is 64K. Using the same as the buffer size for
                    // GZIPInputStream as well.
                    input = new GZIPInputStream(fs.open(source.getPath()), this.bufferSize);
                }
                fsOpened = true;

                output = new BufferedOutputStream(new FileOutputStream(dest));

                while(true) {
                    int read = input.read(buffer);
                    if(read < 0) {
                        break;
                    } else {
                        output.write(buffer, 0, read);
                    }

                    // Update the per file checksum
                    if(fileCheckSumGenerator != null) {
                        fileCheckSumGenerator.update(buffer, 0, read);
                    }

                    // Check if we need to throttle the fetch
                    if(throttler != null) {
                        throttler.maybeThrottle(read);
                    }

                    stats.recordBytes(read);
                    totalBytesRead += read;
                    if(stats.getBytesSinceLastReport() > reportingIntervalBytes) {
                        NumberFormat format = NumberFormat.getNumberInstance();
                        format.setMaximumFractionDigits(2);
                        logger.info(stats.getTotalBytesCopied() / (1024 * 1024) + " MB copied at "
                                    + format.format(stats.getBytesPerSecond() / (1024 * 1024))
                                    + " MB/sec - " + format.format(stats.getPercentCopied())
                                    + " % complete, destination:" + dest);
                        if(this.status != null) {
                            this.status.setStatus(stats.getTotalBytesCopied()
                                                  / (1024 * 1024)
                                                  + " MB copied at "
                                                  + format.format(stats.getBytesPerSecond()
                                                                  / (1024 * 1024)) + " MB/sec - "
                                                  + format.format(stats.getPercentCopied())
                                                  + " % complete, destination:" + dest);
                        }
                        stats.reset();
                    }
                }
                stats.reportFileDownloaded(dest,
                                           startTimeMS,
                                           source.getSize(),
                                           System.currentTimeMillis() - startTimeMS,
                                           attempt,
                                           totalBytesRead);
                logger.info("Completed copy of " + source + " to " + dest);

            } catch(Throwable te) {
                success = false;
                if(!fsOpened) {
                    logger.error("Error while opening the file stream to " + source, te);
                } else {
                    logger.error("Error while copying file " + source + " after " + totalBytesRead
                                 + " bytes.", te);
                }
                if(te.getCause() != null) {
                    logger.error("Cause of error ", te.getCause());
                }

                if(attempt < maxAttempts - 1) {
                    logger.info("Will retry copying after " + retryDelayMs + " ms");
                    sleepForRetryDelayMs();
                } else {
                    stats.reportFileError(dest, maxAttempts, startTimeMS, te);
                    logger.info("Fetcher giving up copy after " + maxAttempts + " attempts");
                    throw te;
                }
            } finally {
                IOUtils.closeQuietly(output);
                IOUtils.closeQuietly(input);
                if(success) {
                    break;
                }
            }
            logger.debug("Completed copy of " + source + " to " + dest);
        }
        return fileCheckSumGenerator;
    }

    private long sizeOfPath(FileSystem fs, Path path) throws IOException {
        long size = 0;
        FileStatus[] statuses = fs.listStatus(path);
        if(statuses != null) {
            for(FileStatus status: statuses) {
                if(status.isDir())
                    size += sizeOfPath(fs, status.getPath());
                else
                    size += status.getLen();
            }
        }
        return size;
    }

    public void setAsyncOperationStatus(AsyncOperationStatus status) {
        this.status = status;
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
        Path p = new Path(url);

        final Configuration config = new Configuration();
        final URI uri = new URI(url);
        config.setInt("io.file.buffer.size", VoldemortConfig.DEFAULT_BUFFER_SIZE);
        config.set("hadoop.rpc.socket.factory.class.ClientProtocol",
                   ConfigurableSocketFactory.class.getName());
        config.setInt("io.socket.receive.buffer", 1 * 1024 * 1024 - 10000);

        FileSystem fs = null;
        p = new Path(url);
        HdfsFetcher.keytabPath = keytabLocation;
        HdfsFetcher.kerberosPrincipal = kerberosUser;

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
        HdfsFetcher fetcher = new HdfsFetcher(null,
                                              maxBytesPerSec,
                                              VoldemortConfig.REPORTING_INTERVAL_BYTES,
                                              VoldemortConfig.DEFAULT_BUFFER_SIZE,
                                              0,
                                              keytabLocation,
                                              kerberosUser,
                                              5,
                                              5000,
                                              true,
                                              50);
        long start = System.currentTimeMillis();
        if(destDir == null)
            destDir = System.getProperty("java.io.tmpdir") + File.separator + start;

        File location = fetcher.fetch(url, destDir, hadoopPath);

        double rate = size * Time.MS_PER_SECOND / (double) (System.currentTimeMillis() - start);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(2);
        System.out.println("Fetch to " + location + " completed: "
                           + nf.format(rate / (1024.0 * 1024.0)) + " MB/sec.");
        fs.close();
    }
}
