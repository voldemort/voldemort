package voldemort.contrib.fetcher;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import voldemort.store.readonly.FileFetcher;

/**
 * Fetch the store from HDFS
 * 
 * @author jay
 * 
 */
public class HdfsFetcher implements FileFetcher {

    public File fetchFile(String fileUrl) throws IOException {
        Path filePath = new Path(fileUrl);
        FileSystem fs = filePath.getFileSystem(new Configuration());

        // copy index file
        FSDataInputStream input = null;
        OutputStream output = null;
        try {
            input = fs.open(filePath);
            File outputFile = File.createTempFile("fetcher-", ".dat");
            output = new FileOutputStream(outputFile);
            IOUtils.copyLarge(input, output);
            return outputFile;
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(input);
        }
    }

}
