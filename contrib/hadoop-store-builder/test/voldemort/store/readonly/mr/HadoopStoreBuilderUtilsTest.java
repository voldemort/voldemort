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

public class HadoopStoreBuilderUtilsTest {

    @Test
    public void testGetReplicaCount() throws IOException {
        Path testPath = new Path(TestUtils.createTempDir().getAbsolutePath());
        FileSystem fs = testPath.getFileSystem(new Configuration());
        fs.mkdirs(testPath);

        assertEquals(HadoopStoreBuilderUtils.getChunkFiles(fs, testPath, 0, 0).length, 0);

        fs.create(new Path(testPath, "0_0_1"));
        assertEquals(HadoopStoreBuilderUtils.getChunkFiles(fs, testPath, 0, 0).length, 1);
        assertEquals(HadoopStoreBuilderUtils.getChunkFiles(fs, testPath, 0, 1).length, 0);

        fs.create(new Path(testPath, "1_0_0"));
        assertEquals(HadoopStoreBuilderUtils.getChunkFiles(fs, testPath, 1, 0).length, 1);

        fs.create(new Path(testPath, "1_0_1"));
        assertEquals(HadoopStoreBuilderUtils.getChunkFiles(fs, testPath, 1, 0).length, 2);

        fs.create(new Path(testPath, "1_0_2"));
        assertEquals(HadoopStoreBuilderUtils.getChunkFiles(fs, testPath, 1, 0).length, 3);
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
        byte[] randomBytes = TestUtils.randomBytes(10);

        // Write file contents
        FileOutputStream stream = new FileOutputStream(testPath.toString());
        stream.write(randomBytes);
        stream.close();

        // Read back empty file
        Assert.assertEquals(HadoopStoreBuilderUtils.readFileContents(fs, testPath, 1024),
                            new String(randomBytes));

        // 3) Write a json string
        fs.delete(testPath, true);
        fs.create(testPath);

        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());

        // Write file contents
        stream = new FileOutputStream(testPath.toString());
        stream.write(metadata.toJsonString().getBytes());
        stream.close();

        ReadOnlyStorageMetadata readMetadata = new ReadOnlyStorageMetadata(HadoopStoreBuilderUtils.readFileContents(fs,
                                                                                                                    testPath,
                                                                                                                    1024));
        Assert.assertEquals(readMetadata.get(ReadOnlyStorageMetadata.FORMAT),
                            ReadOnlyStorageFormat.READONLY_V2.getCode());
    }

}
