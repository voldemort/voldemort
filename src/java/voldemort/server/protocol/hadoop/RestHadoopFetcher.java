package voldemort.server.protocol.hadoop;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.rest.client.RestFSException;
import org.apache.hadoop.fs.rest.client.RestFileStatus;
import org.apache.hadoop.fs.rest.client.RestFileSystem;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.ByteUtils;
import voldemort.utils.DynamicEventThrottler;
import voldemort.utils.DynamicThrottleLimit;
import voldemort.utils.EventThrottler;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;

public class RestHadoopFetcher implements FileFetcher {

    private static final Logger logger = Logger.getLogger(RestHadoopFetcher.class);
    private static final AtomicInteger copyCount = new AtomicInteger(0);
    private static final int NUM_RETRIES = 3;

    private final Long maxBytesPerSecond, reportingIntervalBytes;
    private EventThrottler throttler = null;
    private DynamicThrottleLimit globalThrottleLimit = null;
    private final int bufferSize;
    private AsyncOperationStatus status;
    private long minBytesPerSecond = 0;
    private String kerberosPrincipal = VoldemortConfig.DEFAULT_KERBEROS_PRINCIPAL;
    private String keytabPath = "";
    private String kerberosKdc = "";
    private String kerberosRealm = "";
    private VoldemortConfig voldemortConfig = null;

    public RestHadoopFetcher(VoldemortConfig config) {
        this(null,
             null,
             config.getReadOnlyFetcherReportingIntervalBytes(),
             config.getFetcherBufferSize(),
             config.getReadOnlyFetcherMinBytesPerSecond(),
             config.getReadOnlyKeytabPath(),
             config.getReadOnlyKerberosUser(),
             config.getReadOnlyKerberosKdc(),
             config.getReadOnlyKerberosRealm());

        this.voldemortConfig = config;

        logger.info("Created hdfs fetcher with no dynamic throttler, buffer size " + bufferSize
                    + ", reporting interval bytes " + reportingIntervalBytes);
    }

    public RestHadoopFetcher(VoldemortConfig config, DynamicThrottleLimit dynThrottleLimit) {
        this(dynThrottleLimit,
             null,
             config.getReadOnlyFetcherReportingIntervalBytes(),
             config.getFetcherBufferSize(),
             config.getReadOnlyFetcherMinBytesPerSecond(),
             config.getReadOnlyKeytabPath(),
             config.getReadOnlyKerberosUser(),
             config.getReadOnlyKerberosKdc(),
             config.getReadOnlyKerberosRealm());

        this.voldemortConfig = config;

        logger.info("Created hdfs fetcher with throttle rate " + dynThrottleLimit.getRate()
                    + ", buffer size " + bufferSize + ", reporting interval bytes "
                    + reportingIntervalBytes);
    }

    public RestHadoopFetcher() {
        this((Long) null,
             VoldemortConfig.REPORTING_INTERVAL_BYTES,
             VoldemortConfig.DEFAULT_BUFFER_SIZE);
    }

    public RestHadoopFetcher(Long maxBytesPerSecond, Long reportingIntervalBytes, int bufferSize) {
        this(null, maxBytesPerSecond, reportingIntervalBytes, bufferSize, 0, "", "", "", "");
    }

    public RestHadoopFetcher(DynamicThrottleLimit dynThrottleLimit,
                             Long maxBytesPerSecond,
                             Long reportingIntervalBytes,
                             int bufferSize,
                             long minBytesPerSecond,
                             String keytabLocation,
                             String kerberosUser,
                             String kerberosKdc,
                             String kerberosRealm) {
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
        this.kerberosPrincipal = kerberosUser;
        this.keytabPath = keytabLocation;
        this.kerberosKdc = kerberosKdc;
        this.kerberosRealm = kerberosRealm;
    }

    public File fetch(String sourceFileUrl, String destinationFile, String hadoopConfigPath)
            throws IOException, RestFSException {
        // authentication
        RestHadoopAuth.loginIfNot();

        // add job to the throttler
        addThrottledJob();

        ObjectName jmxName = null;
        try {
            URL sourceUrl = new URL(sourceFileUrl);

            // instantiate RestFS
            RestFileSystem rfs = new RestFileSystem(sourceUrl.getProtocol() + "://"
                                                    + sourceUrl.getHost() + ":"
                                                    + sourceUrl.getPort());

            String fullyQualifiedFileName = sourceUrl.getFile();
            CopyStats stats = new CopyStats(fullyQualifiedFileName,
                                            sizeOfPath(rfs, fullyQualifiedFileName));
            jmxName = JmxUtils.registerMbean("hdfs-copy-" + copyCount.getAndIncrement(), stats);
            File destination = new File(destinationFile);

            if(destination.exists()) {
                throw new VoldemortException("Version directory " + destination.getAbsolutePath()
                                             + " already exists");
            }

            logger.info("Starting fetch for : " + sourceFileUrl);
            boolean result = fetch(rfs, fullyQualifiedFileName, destination, stats);
            logger.info("Completed fetch : " + sourceFileUrl);

            if(result) {
                return destination;
            } else {
                return null;
            }
        } catch(IOException e) {
            e.printStackTrace();
            logger.error("Error while getting Hadoop filesystem : " + e);
            throw new VoldemortException("Error while getting Hadoop filesystem : " + e);
        } catch(Throwable te) {
            te.printStackTrace();
            logger.error("Error thrown while trying to get Hadoop filesystem");
            throw new VoldemortException("Error thrown while trying to get Hadoop filesystem : "
                                         + te);
        } finally {
            removeThrottledJob();
            if(jmxName != null)
                JmxUtils.unregisterMbean(jmxName);
        }
    }

    @Override
    public File fetch(String source, String dest) throws IOException {
        File file = null;
        try {
            file = fetch(source, dest, null);
        } catch(RestFSException e) {
            logger.error(e.getMessage());
        } catch(IOException IOExceptions) {
            throw IOExceptions;
        }
        return file;
    }

    @Override
    public void setAsyncOperationStatus(AsyncOperationStatus status) {}

    private void addThrottledJob() throws VoldemortException {
        if(this.globalThrottleLimit != null) {
            if(this.globalThrottleLimit.getSpeculativeRate() < this.minBytesPerSecond)
                throw new VoldemortException("Too many push jobs.");
            this.globalThrottleLimit.incrementNumJobs();
        }
    }

    private void removeThrottledJob() {
        if(this.globalThrottleLimit != null) {
            this.globalThrottleLimit.decrementNumJobs();
        }
    }

    private long sizeOfPath(RestFileSystem rfs, String path) throws IOException, RestFSException {
        long size = 0;
        RestFileStatus[] statuses = rfs.listStatus(path).toArray();
        if(statuses != null) {
            for(RestFileStatus status: statuses) {
                if(status.isDir())
                    size += sizeOfPath(rfs, path + File.separator + status.getPathSuffix());
                else
                    size += status.getLength();
            }
        }
        return size;
    }

    /**
     * Function to copy a file from the given filesystem with a checksum of type
     * 'checkSumType' computed and returned. In case an error occurs during such
     * a copy, we do a retry for a maximum of NUM_RETRIES
     * 
     * @param rfs RestFilesystem used to copy the file
     * @param source Source path of the file to copy
     * @param dest Destination path of the file on the local machine
     * @param stats Stats for measuring the transfer progress
     * @param checkSumType Type of the Checksum to be computed for this file
     * @return A Checksum (generator) of type checkSumType which contains the
     *         computed checksum of the copied file
     * @throws IOException
     */
    private CheckSum copyFileWithCheckSum(RestFileSystem rfs,
                                          String source,
                                          File dest,
                                          CopyStats stats,
                                          CheckSumType checkSumType) throws IOException {
        CheckSum fileCheckSumGenerator = null;
        logger.info("Starting copy of " + source + " to " + dest);
        BufferedInputStream input = null;
        OutputStream output = null;

        // TODO: remove the retry logic, have RestClient take care of the retry
        // logic.
        for(int attempt = 0; attempt < NUM_RETRIES; attempt++) {
            boolean success = true;
            try {

                // Create a per file checksum generator
                if(checkSumType != null) {
                    fileCheckSumGenerator = CheckSum.getInstance(checkSumType);
                }

                input = new BufferedInputStream(rfs.openFile(source).getInputStream());
                output = new BufferedOutputStream(new FileOutputStream(dest));
                byte[] buffer = new byte[bufferSize];
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
                logger.info("Completed copy of " + source + " to " + dest);

            } catch(IOException ioe) {
                success = false;
                logger.error("Error during copying file ", ioe);
                ioe.printStackTrace();
                if(attempt < NUM_RETRIES - 1) {
                    logger.info("retrying copying");
                } else {
                    throw ioe;
                }

            } catch(Exception e) {
                logger.error("Error during copying file ", e);
                return null;

            } catch(Throwable te) {
                logger.error("Error during copying file ", te);
                return null;

            }
            // the finally block _always_ executes even if we have
            // return in the catch block

            finally {
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

    // TODO: need method to check if a given source leads to a file or a
    // directory
    private boolean fetch(RestFileSystem rfs, String source, File dest, CopyStats stats)
            throws IOException, RestFSException {
        boolean fetchSucceed = false;

        if(rfs.fileStatus(source).isDir()) {
            Utils.mkdirs(dest);
            RestFileStatus statuses[] = rfs.listStatus(source).toArray();
            if(statuses != null && statuses.length > 0) {
                // sort the files so that index files come last. Maybe
                // this will help keep them cached until the swap
                Arrays.sort(statuses, new IndexFileLastComparator());
                byte[] origCheckSum = null;
                CheckSumType checkSumType = CheckSumType.NONE;

                // Do a checksum of checksum - Similar to HDFS
                CheckSum checkSumGenerator = null;
                CheckSum fileCheckSumGenerator = null;

                for(RestFileStatus status: statuses) {
                    String fileNameWithAbsolutePath = status.getAbsolutePath();
                    String shortFileName = status.getPathSuffix();
                    logger.info("fetching file: " + fileNameWithAbsolutePath);
                    // Kept for backwards compatibility
                    if(shortFileName.contains("checkSum.txt")) {

                        logger.warn("Found checksum file in old format: " + shortFileName);

                    } else if(shortFileName.contains(".metadata")) {

                        logger.debug("Reading .metadata");
                        // Read metadata into local file
                        File copyLocation = new File(dest, shortFileName);
                        copyFileWithCheckSum(rfs,
                                             fileNameWithAbsolutePath,
                                             copyLocation,
                                             stats,
                                             null);

                        // Open the local file to initialize checksum
                        ReadOnlyStorageMetadata metadata;
                        try {
                            metadata = new ReadOnlyStorageMetadata(copyLocation);
                        } catch(IOException e) {
                            logger.error("Error reading metadata file ", e);
                            throw new VoldemortException(e);
                        }

                        // Read checksum
                        String checkSumTypeString = (String) metadata.get(ReadOnlyStorageMetadata.CHECKSUM_TYPE);
                        String checkSumString = (String) metadata.get(ReadOnlyStorageMetadata.CHECKSUM);

                        if(checkSumTypeString != null && checkSumString != null) {

                            try {
                                origCheckSum = Hex.decodeHex(checkSumString.toCharArray());
                            } catch(DecoderException e) {
                                logger.error("Exception reading checksum file. Ignoring checksum ",
                                             e);
                                continue;
                            }

                            logger.debug("Checksum from .metadata "
                                         + new String(Hex.encodeHex(origCheckSum)));

                            // Define the Global checksum generator
                            checkSumType = CheckSum.fromString(checkSumTypeString);
                            checkSumGenerator = CheckSum.getInstance(checkSumType);
                        }

                    } else if(!shortFileName.startsWith(".")) {

                        // Read other (.data , .index files)
                        File copyLocation = new File(dest, shortFileName);
                        fileCheckSumGenerator = copyFileWithCheckSum(rfs,
                                                                     fileNameWithAbsolutePath,
                                                                     copyLocation,
                                                                     stats,
                                                                     checkSumType);

                        if(fileCheckSumGenerator != null && checkSumGenerator != null) {
                            byte[] checkSum = fileCheckSumGenerator.getCheckSum();
                            if(logger.isDebugEnabled()) {
                                logger.debug("Checksum for " + shortFileName + " - "
                                             + new String(Hex.encodeHex(checkSum)));
                            }
                            checkSumGenerator.update(checkSum);
                        }
                    }

                }

                logger.info("Completed reading all files from " + source.toString() + " to "
                            + dest.getAbsolutePath());
                // Check checksum
                if(checkSumType != CheckSumType.NONE) {
                    byte[] newCheckSum = checkSumGenerator.getCheckSum();
                    boolean checkSumComparison = (ByteUtils.compare(newCheckSum, origCheckSum) == 0);

                    logger.info("Checksum generated from streaming - "
                                + new String(Hex.encodeHex(newCheckSum)));
                    logger.info("Checksum on file - " + new String(Hex.encodeHex(origCheckSum)));
                    logger.info("Check-sum verification - " + checkSumComparison);

                    fetchSucceed = checkSumComparison;
                } else {
                    logger.info("No check-sum verification required");
                    fetchSucceed = true;
                }
            } else {
                logger.error("No files found under the source location: " + source);
            }
        } else {
            logger.error("Source " + source + " should be a directory");
        }
        return fetchSucceed;
    }

    public static class CopyStats {

        private final String fileName;
        private volatile long bytesSinceLastReport;
        private volatile long totalBytesCopied;
        private volatile long lastReportNs;
        private volatile long totalBytes;

        public CopyStats(String fileName, long totalBytes) {
            this.fileName = fileName;
            this.totalBytesCopied = 0L;
            this.bytesSinceLastReport = 0L;
            this.totalBytes = totalBytes;
            this.lastReportNs = System.nanoTime();
        }

        public void recordBytes(long bytes) {
            this.totalBytesCopied += bytes;
            this.bytesSinceLastReport += bytes;
        }

        public void reset() {
            this.bytesSinceLastReport = 0;
            this.lastReportNs = System.nanoTime();
        }

        public long getBytesSinceLastReport() {
            return bytesSinceLastReport;
        }

        public double getPercentCopied() {
            if(totalBytes == 0) {
                return 0.0;
            } else {
                return (double) (totalBytesCopied * 100) / (double) totalBytes;
            }
        }

        @JmxGetter(name = "totalBytesCopied", description = "The total number of bytes copied so far in this transfer.")
        public long getTotalBytesCopied() {
            return totalBytesCopied;
        }

        @JmxGetter(name = "bytesPerSecond", description = "The rate of the transfer in bytes/second.")
        public double getBytesPerSecond() {
            double ellapsedSecs = (System.nanoTime() - lastReportNs) / (double) Time.NS_PER_SECOND;
            return bytesSinceLastReport / ellapsedSecs;
        }

        @JmxGetter(name = "filename", description = "The file path being copied.")
        public String getFilename() {
            return this.fileName;
        }
    }

    /**
     * A comparator that sorts index files last. This is a heuristic for
     * retaining the index file in page cache until the swap occurs
     * 
     */
    public static class IndexFileLastComparator implements Comparator<RestFileStatus> {

        public int compare(RestFileStatus fs1, RestFileStatus fs2) {
            // directories before files
            if(fs1.isDir())
                return fs2.isDir() ? 0 : -1;
            if(fs2.isDir())
                return fs1.isDir() ? 0 : 1;

            String f1 = fs1.getPathSuffix(), f2 = fs2.getPathSuffix();

            // All metadata files given priority
            if(f1.endsWith("metadata"))
                return -1;
            if(f2.endsWith("metadata"))
                return 1;

            // if both same, lexicographically
            if((f1.endsWith(".index") && f2.endsWith(".index"))
               || (f1.endsWith(".data") && f2.endsWith(".data"))) {
                return f1.compareToIgnoreCase(f2);
            }

            if(f1.endsWith(".index")) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    /*
     * Main method for testing fetching
     */
    public static void main(String[] args) throws Exception {
        if(args.length < 5)
            Utils.croak("USAGE: java "
                        + RestHadoopFetcher.class.getName()
                        + " [url] [keytab location] [kerberos username] [kerberos realm] [kerberos kdc]");
        String url = args[0];
        String keytabLocation = args[1];
        String kerberosUser = args[2];
        String realm = args[3];
        String kdc = args[4];
        long maxBytesPerSec = 1024 * 1024 * 1024;

        // login
        RestHadoopAuth restAuth = new RestHadoopAuth(realm, kdc, kerberosUser, keytabLocation);
        restAuth.start();

        RestHadoopFetcher fetcher = new RestHadoopFetcher(null,
                                                          maxBytesPerSec,
                                                          VoldemortConfig.REPORTING_INTERVAL_BYTES,
                                                          VoldemortConfig.DEFAULT_BUFFER_SIZE,
                                                          0,
                                                          keytabLocation,
                                                          kerberosUser,
                                                          realm,
                                                          kdc);

        // start file fetching
        long start = System.currentTimeMillis();
        File location = fetcher.fetch(url, System.getProperty("user.home") + File.separator + start);

        // complete file fetching; print stats
        long size = location.length();
        double rate = size * Time.MS_PER_SECOND / (double) (System.currentTimeMillis() - start);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(2);
        System.out.println("Fetch to " + location + " completed: "
                           + nf.format(rate / (1024.0 * 1024.0)) + " MB/sec.");

        // logout
        restAuth.stop();
    }
}
