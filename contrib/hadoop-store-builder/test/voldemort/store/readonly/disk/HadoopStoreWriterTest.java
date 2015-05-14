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
import voldemort.xml.StoreDefinitionsMapper;

@RunWith(Parameterized.class)
/**
 * Tight assumptions this test makes: nodeId = 1, partitionId = 1,
 * replicaType = 1, chunkId = 1
 *
 * So files generated will be either 1_1_1.data (if saveKeys == true) or
 * 1_1.data (if saveKeys == false)
 *
 * The only compression type allowed is gzip.
 *
 *
 * Also this test suite does not check for checksum types. It assuems a
 * default checksum type of NONE
 */
public class HadoopStoreWriterTest {

    private JobConf conf = null;
    private HadoopStoreWriter hadoopStoreWriter = null;
    File tmpOutPutDirectory = null;
    BytesWritable globalKey;
    ArrayList<BytesWritable> globalValueList;
    boolean saveKeys;
    File dataFile, indexFile;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    public HadoopStoreWriterTest(boolean saveKeys) {
        this.saveKeys = saveKeys;
    }

    private void init() {
        tmpOutPutDirectory = TestUtils.createTempDir();

        // Setup before each test method
        conf = new JobConf();
        conf.setInt("num.chunks", 1);
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

    private BytesWritable generateKey(String key) {
        byte[] keyBytes = key.getBytes();
        return new BytesWritable(keyBytes);
    }

    private void generateUnCompressedFiles(boolean saveKeys) throws IOException {
        conf.setBoolean("save.keys", saveKeys);
        conf.setBoolean("reducer.output.compress", false);
        hadoopStoreWriter = new HadoopStoreWriter(conf);
        String key = "test_key_1";
        String value = "test_value_1";
        globalKey = generateKey(key);
        globalValueList = ReadOnlyTestUtils.generateValues(key, value, saveKeys);
        hadoopStoreWriter.write(globalKey, globalValueList.iterator(), null);
        hadoopStoreWriter.close();
    }

    private void generateExpectedDataAndIndexFileNames(boolean isGzipCompressed) {
        String basePath = tmpOutPutDirectory.getAbsolutePath() + File.separator + "node-1"
                          + File.separator;
        String fileName = "";
        String dataFilePrefix = KeyValueWriter.DATA_FILE_EXTENSION;
        String indexFilePrefix = KeyValueWriter.INDEX_FILE_EXTENSION;
        String fileExtension = "";

        if(isGzipCompressed) {
            fileExtension = KeyValueWriter.GZIP_FILE_EXTENSION;
        }
        fileName = (saveKeys ? "1_1_1" : "1_1");
        dataFile = new File(basePath + fileName + dataFilePrefix + fileExtension);
        indexFile = new File(basePath + fileName + indexFilePrefix + fileExtension);
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

        generateUnCompressedFiles(saveKeys);
        generateExpectedDataAndIndexFileNames(false);

        if(dataFile.exists() && dataFile.isFile()) {
            if(dataFile.length() == 0) {
                Assert.fail("Expty data file");
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
        cleanUp();

    }

    @Test
    public void testCompressedWriter() throws IOException {

        /**
         * 1. Run uncompressed path for getting baseline file
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

        generateUnCompressedFiles(saveKeys);
        generateExpectedDataAndIndexFileNames(false);
        conf.setBoolean("save.keys", saveKeys);
        conf.setBoolean("reducer.output.compress", true);
        conf.setStrings("reducer.output.compress.codec", KeyValueWriter.COMPRESSION_CODEC);

        hadoopStoreWriter = new HadoopStoreWriter(conf);
        hadoopStoreWriter.write(globalKey, globalValueList.iterator(), null);
        hadoopStoreWriter.close();

        // reset dataFile and indexFile to point to the compressed files being
        // generated
        generateExpectedDataAndIndexFileNames(true);

        String decompressedDataFile = dataFile.getAbsolutePath() + "_decompressed";
        ReadOnlyTestUtils.unGunzipFile(dataFile.getAbsolutePath(), decompressedDataFile);

        String decompressedIndexFile = indexFile.getAbsolutePath() + "_decompressed";
        ReadOnlyTestUtils.unGunzipFile(indexFile.getAbsolutePath(), decompressedIndexFile);

        // reset data and index file to point to the uncompressed files that
        // were initially generated.
        generateExpectedDataAndIndexFileNames(false);
        Assert.assertTrue(ReadOnlyTestUtils.areTwoBinaryFilesEqual(dataFile,
                                                                   new File(decompressedDataFile)));
        Assert.assertTrue(ReadOnlyTestUtils.areTwoBinaryFilesEqual(indexFile,
                                                                   new File(decompressedIndexFile)));

        cleanUp();
    }

}
