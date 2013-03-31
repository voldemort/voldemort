package voldemort.store.bdb.dataconversion;

import java.util.List;

import voldemort.store.StoreBinaryFormat;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;
import voldemort.versioning.Versioned;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class BdbConvertNewDupToPidScan extends AbstractBdbConversion {

    BdbConvertNewDupToPidScan(String storeName,
                              String clusterXmlPath,
                              String sourceEnvPath,
                              String destEnvPath,
                              int logFileSize,
                              int nodeMax) throws Exception {
        super(storeName, clusterXmlPath, sourceEnvPath, destEnvPath, logFileSize, nodeMax);
    }

    @Override
    public void transfer() throws Exception {
        cursor = srcDB.openCursor(null, null);
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        HashFunction hash = new FnvHashFunction();
        int totalPartitions = cluster.getNumberOfPartitions();

        List<Versioned<byte[]>> vals;
        long startTime = System.currentTimeMillis();
        int scanCount = 0;
        int keyCount = 0;
        while(cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
            keyCount++;

            vals = StoreBinaryFormat.fromByteArray(valueEntry.getData());
            scanCount += vals.size();

            int partition = BdbConvertData.abs(hash.hash(keyEntry.getData()))
                            % (Math.max(1, totalPartitions));

            OperationStatus putStatus = dstDB.put(null,
                                                  new DatabaseEntry(StoreBinaryFormat.makePrefixedKey(keyEntry.getData(),
                                                                                                      partition)),
                                                  valueEntry);
            if(OperationStatus.SUCCESS != putStatus) {
                String errorStr = "Put failed with " + putStatus + " for key"
                                  + BdbConvertData.writeAsciiString(keyEntry.getData());
                logger.error(errorStr);
                throw new Exception(errorStr);
            }

            if(scanCount % 1000000 == 0)
                logger.info("Reverted " + scanCount + "entries in "
                            + (System.currentTimeMillis() - startTime) / 1000 + " secs");
        }
        logger.info("Converted " + scanCount + "entries and " + keyCount + " keys in "
                    + (System.currentTimeMillis() - startTime) / 1000 + " secs");
    }

    @Override
    public boolean areDuplicatesNeededForSrc() {
        return false;
    }

    @Override
    public boolean areDuplicatesNeededForDest() {
        return false;
    }
}
