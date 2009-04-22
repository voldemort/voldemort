package voldemort.contrib.fetcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.commons.io.IOUtils;

import voldemort.utils.ByteUtils;

import junit.framework.TestCase;

public class HdfsFetcherTest extends TestCase {

    private File testFile;
    private HdfsFetcher fetcher;

    @Override
    public void setUp() throws IOException {
        this.testFile = File.createTempFile("test", ".dat");
        Random random = new Random();
        byte[] buffer = new byte[1000];
        random.nextBytes(buffer);
        this.fetcher = new HdfsFetcher();
    }

    public void testFetch() throws IOException {
        File fetchedFile = this.fetcher.fetchFile(testFile.getAbsolutePath());
        InputStream orig = new FileInputStream(testFile);
        byte[] origBytes = IOUtils.toByteArray(orig);
        InputStream fetched = new FileInputStream(fetchedFile);
        byte[] fetchedBytes = IOUtils.toByteArray(fetched);
        assertTrue("Fetched bytes not equal to original bytes.",
                   0 == ByteUtils.compare(origBytes, fetchedBytes));
    }

}
