package voldemort.store.readonly.mr;

import static org.junit.Assert.assertEquals;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.chunk.DataFileChunkSet;

public class HadoopStoreBuilderUtilsTest {

    @Test
    public void testGetReplicaCount() throws IOException {
        Path testPath = new Path(TestUtils.createTempDir().getAbsolutePath());
        FileSystem fs = testPath.getFileSystem(new Configuration());
        fs.mkdirs(testPath);

        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 0, 0).length, 0);

        fs.create(new Path(testPath, "0_0_1.data"));
        fs.create(new Path(testPath, "0_0_1data"));
        fs.create(new Path(testPath, "0_0_2.index"));
        fs.create(new Path(testPath, "0_0.data"));
        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 0, 0).length, 1);
        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 0, 0, 1).length, 1);
        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 1, 0, 1).length, 0);
        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 0, 1).length, 0);

        fs.create(new Path(testPath, "1_0_0.data"));
        fs.create(new Path(testPath, "1_0"));
        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 1, 0).length, 1);
        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 1, 0, 0).length, 1);

        fs.create(new Path(testPath, "1_0_1.data"));
        fs.create(new Path(testPath, "1_0_1data"));
        fs.create(new Path(testPath, "1_0_1.index"));
        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 1, 0).length, 2);
        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 1, 0, 1).length, 1);

        fs.create(new Path(testPath, "1_0_2.data"));
        assertEquals(HadoopStoreBuilderUtils.getDataChunkFiles(fs, testPath, 1, 0).length, 3);
    }

    @Test
    public void testGetDataFileChunkSet() throws IOException {

        Path headPath = new Path(TestUtils.createTempDir().getAbsolutePath());
        Path testPath = new Path(headPath, "0_0_100.data");
        Path junkPath = new Path(headPath, "1_1_100.data");
        FileSystem fs = testPath.getFileSystem(new Configuration());

        // 1) Just one correct file
        fs.create(testPath);
        fs.create(junkPath);
        writeRandomData(testPath, 100);
        DataFileChunkSet set = HadoopStoreBuilderUtils.getDataFileChunkSet(fs,
                                                                           HadoopStoreBuilderUtils.getDataChunkFiles(fs,
                                                                                                                     headPath,
                                                                                                                     0,
                                                                                                                     0));
        assertEquals(set.getNumChunks(), 1);
        assertEquals(set.getDataFileSize(0), 100);

        // 2) Another correct file
        testPath = new Path(headPath, "0_0_99.data");
        fs.create(testPath);
        writeRandomData(testPath, 99);
        set = HadoopStoreBuilderUtils.getDataFileChunkSet(fs,
                                                          HadoopStoreBuilderUtils.getDataChunkFiles(fs,
                                                                                                    headPath,
                                                                                                    0,
                                                                                                    0));
        assertEquals(set.getNumChunks(), 2);
        assertEquals(set.getDataFileSize(0), 99);
        assertEquals(set.getDataFileSize(1), 100);

        // 3) Add some more files
        testPath = new Path(headPath, "0_0_1.data");
        fs.create(testPath);
        writeRandomData(testPath, 1);

        testPath = new Path(headPath, "0_0_10.data");
        fs.create(testPath);
        writeRandomData(testPath, 10);

        testPath = new Path(headPath, "0_0_999.data");
        fs.create(testPath);
        writeRandomData(testPath, 999);

        testPath = new Path(headPath, "0_0_101.data");
        fs.create(testPath);
        writeRandomData(testPath, 101);

        testPath = new Path(headPath, "0_0_1000.data");
        fs.create(testPath);
        writeRandomData(testPath, 1000);

        set = HadoopStoreBuilderUtils.getDataFileChunkSet(fs,
                                                          HadoopStoreBuilderUtils.getDataChunkFiles(fs,
                                                                                                    headPath,
                                                                                                    0,
                                                                                                    0));
        assertEquals(set.getNumChunks(), 7);
        assertEquals(set.getDataFileSize(0), 1);
        assertEquals(set.getDataFileSize(1), 10);
        assertEquals(set.getDataFileSize(2), 99);
        assertEquals(set.getDataFileSize(3), 100);
        assertEquals(set.getDataFileSize(4), 101);
        assertEquals(set.getDataFileSize(5), 999);
        assertEquals(set.getDataFileSize(6), 1000);

    }

    /**
     * Write random numBytes bytes to the file path specified
     * 
     * @param path The path to which we write
     * @param numBytes Number of bytes to write
     * @return Returns the bytes written
     * @throws IOException
     */
    private byte[] writeRandomData(Path path, int numBytes) throws IOException {
        byte[] randomBytes = TestUtils.randomBytes(numBytes);

        // Write file contents
        FileOutputStream stream = new FileOutputStream(path.toString());
        stream.write(randomBytes);
        stream.close();

        return randomBytes;
    }

    @Test
    public void testReadFileContents() throws Exception {

        Path testPath = new Path(TestUtils.createTempDir().getAbsolutePath(), "tempFile");
        FileSystem fs = testPath.getFileSystem(new Configuration());
        fs.create(testPath);

        // 1) Read back empty file
        String emptyString = HadoopStoreBuilderUtils.readFileContents(fs, testPath, 1024);
        Assert.assertEquals(emptyString.length(), 0);

        // 2) Read back random bytes
        byte[] randomBytes = writeRandomData(testPath, 10);

        // Read back data
        Assert.assertEquals(HadoopStoreBuilderUtils.readFileContents(fs, testPath, 1024),
                            new String(randomBytes));

        // 3) Write a json string
        fs.delete(testPath, true);
        fs.create(testPath);

        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());

        // Write file contents
        new FileOutputStream(testPath.toString()).write(metadata.toJsonString().getBytes());

        ReadOnlyStorageMetadata readMetadata = new ReadOnlyStorageMetadata(HadoopStoreBuilderUtils.readFileContents(fs,
                                                                                                                    testPath,
                                                                                                                    1024));
        Assert.assertEquals(readMetadata.get(ReadOnlyStorageMetadata.FORMAT),
                            ReadOnlyStorageFormat.READONLY_V2.getCode());
    }

}
