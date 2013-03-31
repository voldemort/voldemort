package voldemort.store.bdb.dataconversion;

import java.util.ArrayList;
import java.util.List;

import voldemort.store.StoreBinaryFormat;
import voldemort.utils.ByteUtils;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class BdbConvertBaseToPidScan extends AbstractBdbConversion {

    BdbConvertBaseToPidScan(String storeName,
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

        byte[] prevKey = null;
        List<Versioned<byte[]>> vals = new ArrayList<Versioned<byte[]>>();
        HashFunction hash = new FnvHashFunction();
        int totalPartitions = cluster.getNumberOfPartitions();

        long startTime = System.currentTimeMillis();
        int scanCount = 0;
        int keyCount = 0;
        while(cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
            scanCount++;
            if(scanCount % 1000000 == 0)
                logger.info("Converted " + scanCount + "entries in "
                            + (System.currentTimeMillis() - startTime) / 1000 + " secs");

            // read the value as a versioned Object
            VectorClock clock = new VectorClock(valueEntry.getData());
            byte[] bytes = ByteUtils.copy(valueEntry.getData(),
                                          clock.sizeInBytes(),
                                          valueEntry.getData().length);
            Versioned<byte[]> value = new Versioned<byte[]>(bytes, clock);
            byte[] key = keyEntry.getData();

            if(prevKey != null && (ByteUtils.compare(prevKey, key) != 0)) {
                // there is a new key; write out the buffered values and
                // previous key
                int partition = BdbConvertData.abs(hash.hash(prevKey))
                                % (Math.max(1, totalPartitions));

                OperationStatus putStatus = dstDB.put(null,
                                                      new DatabaseEntry(StoreBinaryFormat.makePrefixedKey(prevKey,
                                                                                                          partition)),
                                                      new DatabaseEntry(StoreBinaryFormat.toByteArray(vals)));
                if(OperationStatus.SUCCESS != putStatus) {
                    String errorStr = "Put failed with " + putStatus + " for key"
                                      + BdbConvertData.writeAsciiString(prevKey);
                    logger.error(errorStr);
                    throw new Exception(errorStr);
                }
                vals = new ArrayList<Versioned<byte[]>>();
                keyCount++;
            }

            vals.add(value);
            prevKey = key;
        }
        if(vals.size() > 0) {
            int partition = BdbConvertData.abs(hash.hash(prevKey)) % (Math.max(1, totalPartitions));
            OperationStatus putStatus = dstDB.put(null,
                                                  new DatabaseEntry(StoreBinaryFormat.makePrefixedKey(prevKey,
                                                                                                      partition)),
                                                  new DatabaseEntry(StoreBinaryFormat.toByteArray(vals)));
            if(OperationStatus.SUCCESS != putStatus) {
                String errorStr = "Put failed with " + putStatus + " for key"
                                  + BdbConvertData.writeAsciiString(prevKey);
                logger.error(errorStr);
                throw new Exception(errorStr);
            }
            keyCount++;
        }
        logger.info("Completed " + scanCount + "entries and " + keyCount + " keys in "
                    + (System.currentTimeMillis() - startTime) / 1000 + " secs");
    }

    @Override
    public boolean areDuplicatesNeededForSrc() {
        return true;
    }

    @Override
    public boolean areDuplicatesNeededForDest() {
        return false;
    }
}
