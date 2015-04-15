package voldemort.store.readonly.disk;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.utils.ReadOnlyTestUtils;
import voldemort.utils.ByteUtils;
import voldemort.xml.StoreDefinitionsMapper;

@RunWith(Parameterized.class)
/**
 * Tight assumptions this test makes: nodeId = 1, partitionId = 1,
 * replicaType = 1
 *
 * Number of chunks = 2
 *
 * So files generated will be like:
 * (if saveKeys == true){
 *   1_1_<chunkId>.data // where chunkId can be 0 or 1 since num of chunks = 2.
 * }else{
 *   1_<chunkId>.data // where chunkId can be 0 or 1 since num of chunks = 2.
 * }
 *
 * The only compression type allowed is gzip.
 *
 *
 * Also this test suite does not check for checksum types. It assumes a
 * default checksum type of NONE
 */
public class HadoopStoreWriterPerBucketTest {

    private JobConf conf = null;
    private HadoopStoreWriterPerBucket hadoopStoreWriterPerBucket = null;
    File tmpOutPutDirectory = null;
    BytesWritable Key1;
    ArrayList<BytesWritable> valueList1;
    boolean saveKeys;
    File dataFile, indexFile;
    int numChunks = 2;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    public HadoopStoreWriterPerBucketTest(boolean saveKeys) {
        this.saveKeys = saveKeys;
    }

    private void init() {
        tmpOutPutDirectory = TestUtils.createTempDir();

        // Setup before each test method
        conf = new JobConf();
        conf.setInt("num.chunks", 2);
        conf.set("final.output.dir", tmpOutPutDirectory.getAbsolutePath());
        conf.set("mapred.output.dir", tmpOutPutDirectory.getAbsolutePath());
        conf.set("mapred.task.id", "1234");

        /*
         * We dont have to test different types of checksums. thats covered in
         * seperate unit test
         */
        conf.set("checksum.type", "NONE");

        // generate a list of storeDefinitions.
        List<StoreDefinition> storeDefList = ServerTestUtils.getStoreDefs(1);
        String storesXML = new StoreDefinitionsMapper().writeStoreList(storeDefList);
        conf.set("stores.xml", storesXML);

    }

    private void cleanUp() throws IOException {
        if(tmpOutPutDirectory != null) {
            FileUtils.deleteDirectory(tmpOutPutDirectory);
        }
    }

    private BytesWritable generateKey(String key, int chunkId) {
        int keyLength = key.getBytes().length;
        byte[] keyBytes = new byte[ByteUtils.SIZE_OF_INT + keyLength];
        ByteUtils.writeInt(keyBytes, chunkId, 0);
        System.arraycopy(key.getBytes(), 0, keyBytes, ByteUtils.SIZE_OF_INT, keyLength);
        return new BytesWritable(keyBytes);
    }

    private void generateUnCompressedFiles(boolean saveKeys, int numChunks) throws IOException {
        conf.setBoolean("save.keys", saveKeys);
        conf.setBoolean("reducer.output.compress", false);
        hadoopStoreWriterPerBucket = new HadoopStoreWriterPerBucket(conf);

        for(int i = 0; i < numChunks; i++) {
            String key = "test_key_" + i;
            String value = "test_value_" + i;

            Key1 = generateKey(key, i);
            valueList1 = ReadOnlyTestUtils.generateValues(key, value, saveKeys);

            hadoopStoreWriterPerBucket.write(Key1, valueList1.iterator(), null);
        }
        hadoopStoreWriterPerBucket.close();
    }

    private void generateExpectedDataAndIndexFileNames(boolean isGzipCompressed, int chunkId) {
        String basePath = tmpOutPutDirectory.getAbsolutePath() + File.separator + "node-1"
                          + File.separator;
        String fileName = "";
        String dataFilePrefix = KeyValueWriter.DATA_FILE_EXTENSION;
        String indexFilePrefix = KeyValueWriter.INDEX_FILE_EXTENSION;
        String fileExtension = "";

        if(isGzipCompressed) {
            fileExtension = KeyValueWriter.GZIP_FILE_EXTENSION;
        }
        fileName = (saveKeys ? "1_1" : "1");
        dataFile = new File(basePath + fileName + "_" + chunkId + dataFilePrefix + fileExtension);
        indexFile = new File(basePath + fileName + "_" + chunkId + indexFilePrefix + fileExtension);
    }

    @Test
    public void testUncompressedWriter() throws IOException {
        /**
         * Very basic assertion. This test case needs improvement later.
         *
         * if(saveKeys) Assert if two files are generated - 1_1_1.data and
         * 1_1_1.index with non-zero size under <tmpOutPutDirectory>/node_1.
         *
         * else Assert if two files are generated - 1_1.data and 1_1.index with
         * non-zero size under <tmpOutPutDirectory>/node_1.
         *
         */
        init();

        generateUnCompressedFiles(saveKeys, numChunks);

        for(int i = 0; i < numChunks; i++) {
            generateExpectedDataAndIndexFileNames(false, i);
            if(dataFile.exists() && dataFile.isFile()) {
                if(dataFile.length() == 0) {
                    Assert.fail("Empty data file");
                }
            } else {
                Assert.fail("There is no data file in the expected directory");
            }
            if(indexFile.exists() && indexFile.isFile()) {
                if(indexFile.length() == 0) {
                    Assert.fail("Empty index file");
                }
            } else {
                Assert.fail("There is no data file in the expected directory");
            }
        }
        cleanUp();

    }

    @Test
    public void testCompressedWriter() throws IOException {

        /**
         * 1. Run uncompressed path for getting baseline files
         *
         * 2. then run the compressed path with the same key and value bytes
         *
         * 3. unzip the compressed files separately into a _decompressed file
         * for both data and index files
         *
         * 4. compare the original file and decompressed files for byte-by-byte
         * equality.
         */
        init();

        generateUnCompressedFiles(saveKeys, numChunks);
        conf.setBoolean("save.keys", saveKeys);
        conf.setBoolean("reducer.output.compress", true);
        conf.setStrings("reducer.output.compress.codec", KeyValueWriter.COMPRESSION_CODEC);

        hadoopStoreWriterPerBucket = new HadoopStoreWriterPerBucket(conf);

        // Write into all chunked files
        for(int i = 0; i < numChunks; i++) {
            String key = "test_key_" + i;
            String value = "test_value_" + i;

            Key1 = generateKey(key, i);
            valueList1 = ReadOnlyTestUtils.generateValues(key, value, saveKeys);

            hadoopStoreWriterPerBucket.write(Key1, valueList1.iterator(), null);
        }

        hadoopStoreWriterPerBucket.close();

        // Generate a decompressed file for each of the compressed file and
        // check if they are equal with the original uncompressed file
        for(int i = 0; i < numChunks; i++) {

            // reset dataFile and indexFile to point to the compressed files
            // being
            // generated
            generateExpectedDataAndIndexFileNames(true, i);

            String decompressedDataFile = dataFile.getAbsolutePath() + "_decompressed";
            ReadOnlyTestUtils.unGunzipFile(dataFile.getAbsolutePath(), decompressedDataFile);

            String decompressedIndexFile = indexFile.getAbsolutePath() + "_decompressed";
            ReadOnlyTestUtils.unGunzipFile(indexFile.getAbsolutePath(), decompressedIndexFile);

            // reset data and index file to point to the uncompressed files that
            // were initially generated.
            generateExpectedDataAndIndexFileNames(false, i);
            Assert.assertTrue(ReadOnlyTestUtils.areTwoBinaryFilesEqual(dataFile,
                                                                       new File(decompressedDataFile)));
            Assert.assertTrue(ReadOnlyTestUtils.areTwoBinaryFilesEqual(indexFile,
                                                                       new File(decompressedIndexFile)));
        }
        cleanUp();
    }
}
