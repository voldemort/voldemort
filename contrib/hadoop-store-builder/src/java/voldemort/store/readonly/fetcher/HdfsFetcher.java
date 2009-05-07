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
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int REPORTING_INTERVAL_BYTES = 100 * 1024 * 1024;

    private final Long maxBytesPerSecond;

    public HdfsFetcher(Props props) {
        this(props.getBytes("fetcher.max.bytes.per.sec"));
    }

    public HdfsFetcher() {
        this((Long) null);
    }

    public HdfsFetcher(Long maxBytesPerSecond) {
        this.maxBytesPerSecond = maxBytesPerSecond;
    }

    public File fetchFile(String fileUrl) throws IOException {
        Path filePath = new Path(fileUrl);
        Configuration config = new Configuration();
        config.setInt("io.file.buffer.size", 64 * 1024);
        FileSystem fs = filePath.getFileSystem(config);
        IoThrottler throttler = null;
        if(maxBytesPerSecond != null)
            throttler = new IoThrottler(maxBytesPerSecond);

        // copy file
        long bytesCopied = 0;
        long bytesSinceLastReport = 0;
        FSDataInputStream input = null;
        OutputStream output = null;
        try {
            input = fs.open(filePath);
            File outputFile = File.createTempFile("fetcher-", ".dat");
            output = new FileOutputStream(outputFile);
            byte[] buffer = new byte[BUFFER_SIZE];
            while(true) {
                int read = input.read(buffer);
                if(read < 0)
                    break;
                output.write(buffer, 0, read);
                if(throttler != null)
                    throttler.maybeThrottle(read);
                bytesSinceLastReport += read;
                bytesCopied += read;
                if(bytesSinceLastReport > REPORTING_INTERVAL_BYTES) {
                    logger.info(bytesCopied / (1024 * 1024) + " MB copied");
                    bytesSinceLastReport = 0;
                }

            }
            return outputFile;
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(input);
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
        HdfsFetcher fetcher = new HdfsFetcher(maxBytesPerSec);
        long start = System.currentTimeMillis();
        fetcher.fetchFile(url);
        double rate = size * Time.MS_PER_SECOND / (double) (System.currentTimeMillis() - start);
        System.out.println("Fetch completed: " + rate + " bytes/sec.");
    }
}
