package voldemort.store.readonly.fetcher;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import voldemort.store.readonly.FileFetcher;
import voldemort.utils.Props;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/**
 * A fetcher that fetches the store files from HDFS
 * 
 * @author jay
 * 
 */
public class HdfsFetcher implements FileFetcher {

    private static final Logger logger = Logger.getLogger(HdfsFetcher.class);
    private static final String DEFAULT_TEMP_DIR = new File(System.getProperty("java.io.tmpdir"),
                                                            "hdfs-fetcher").getAbsolutePath();
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int REPORTING_INTERVAL_BYTES = 100 * 1024 * 1024;

    private File tempDir;
    private final Long maxBytesPerSecond;

    public HdfsFetcher(Props props) {
        this(props.containsKey("fetcher.max.bytes.per.sec") ? props.getBytes("fetcher.max.bytes.per.sec")
                                                           : null,
             new File(props.getString("hdfs.fetcher.tmp.dir", DEFAULT_TEMP_DIR)));
        logger.info("Created hdfs fetcher with temp dir = " + tempDir.getAbsolutePath()
                    + " and throttle rate " + maxBytesPerSecond);
    }

    public HdfsFetcher() {
        this((Long) null, null);
    }

    public HdfsFetcher(Long maxBytesPerSecond, File tempDir) {
        if(tempDir == null)
            this.tempDir = new File(DEFAULT_TEMP_DIR);
        else
            this.tempDir = Utils.notNull(new File(tempDir, "hdfs-fetcher"));
        this.maxBytesPerSecond = maxBytesPerSecond;
        this.tempDir.mkdirs();
    }

    public File fetch(String fileUrl) throws IOException {
        Path path = new Path(fileUrl);
        Configuration config = new Configuration();
        config.setInt("io.file.buffer.size", 64 * 1024);
        FileSystem fs = path.getFileSystem(config);
        IoThrottler throttler = null;
        if(maxBytesPerSecond != null)
            throttler = new IoThrottler(maxBytesPerSecond);

        // copy file
        CopyStats stats = new CopyStats();
        File destination = new File(this.tempDir, path.getName());
        fetch(fs, path, destination, throttler, stats);
        return destination;
    }

    private void fetch(FileSystem fs, Path source, File dest, IoThrottler throttler, CopyStats stats)
            throws IOException {
        if(fs.isFile(source)) {
            copyFile(fs, source, dest, throttler, stats);
        } else {
            dest.mkdirs();
            FileStatus[] statuses = fs.listStatus(source);
            if(statuses != null) {
                for(FileStatus status: statuses) {
                    if(!status.getPath().getName().startsWith(".")) {
                        fetch(fs,
                              status.getPath(),
                              new File(dest, status.getPath().getName()),
                              throttler,
                              stats);
                    }
                }
            }
        }
    }

    private void copyFile(FileSystem fs,
                          Path source,
                          File dest,
                          IoThrottler throttler,
                          CopyStats stats) throws IOException {
        logger.info("Starting copy of " + source + " to " + dest);
        FSDataInputStream input = null;
        OutputStream output = null;
        try {
            input = fs.open(source);
            output = new FileOutputStream(dest);
            byte[] buffer = new byte[BUFFER_SIZE];
            while(true) {
                int read = input.read(buffer);
                if(read < 0)
                    break;
                output.write(buffer, 0, read);
                if(throttler != null)
                    throttler.maybeThrottle(read);
                stats.recordBytes(read);
                if(stats.getBytesSinceLastReport() > REPORTING_INTERVAL_BYTES) {
                    logger.info(stats.getBytesCopied() / (1024 * 1024) + " MB copied");
                    stats.resetBytesSinceLastReport();
                }
            }
            logger.info("Completed copy of " + source + " to " + dest);
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(input);
        }
    }

    private static class CopyStats {

        private long bytesSinceLastReport;
        private long bytesCopied;

        public CopyStats() {
            this.bytesCopied = 0;
            this.bytesSinceLastReport = 0;
        }

        public void recordBytes(long bytes) {
            this.bytesCopied += bytes;
            this.bytesSinceLastReport += bytes;
        }

        public void resetBytesSinceLastReport() {
            this.bytesSinceLastReport = 0;
        }

        public long getBytesSinceLastReport() {
            return bytesSinceLastReport;
        }

        public long getBytesCopied() {
            return bytesCopied;
        }
    }

    /*
     * Main method for testing fetching
     */
    public static void main(String[] args) throws Exception {
        if(args.length != 2)
            Utils.croak("USAGE: java " + HdfsFetcher.class.getName() + " url maxBytesPerSec");
        String url = args[0];
        long maxBytesPerSec = Long.parseLong(args[1]);
        Path p = new Path(url);
        FileStatus status = p.getFileSystem(new Configuration()).getFileStatus(p);
        long size = status.getLen();
        HdfsFetcher fetcher = new HdfsFetcher(maxBytesPerSec, null);
        long start = System.currentTimeMillis();
        File location = fetcher.fetch(url);
        double rate = size * Time.MS_PER_SECOND / (double) (System.currentTimeMillis() - start);
        System.out.println("Fetch to " + location + " completed: " + rate + " bytes/sec.");
    }
}
