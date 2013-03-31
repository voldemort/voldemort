package voldemort.store.bdb.dataconversion;

import java.util.List;

import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.VersionedSerializer;
import voldemort.store.StoreBinaryFormat;
import voldemort.versioning.Versioned;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class BdbRevertPidScanToBase extends AbstractBdbConversion {

    BdbRevertPidScanToBase(String storeName,
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
        VersionedSerializer<byte[]> versionedSerializer = new VersionedSerializer<byte[]>(new IdentitySerializer());

        List<Versioned<byte[]>> vals;
        long startTime = System.currentTimeMillis();

        int scanCount = 0;
        int keyCount = 0;
        while(cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
            keyCount++;

            vals = StoreBinaryFormat.fromByteArray(valueEntry.getData());
            // pull out the real key
            byte[] stripedKey = StoreBinaryFormat.extractKey(keyEntry.getData());

            for(Versioned<byte[]> val: vals) {
                OperationStatus putStatus = dstDB.put(null,
                                                      new DatabaseEntry(stripedKey),
                                                      new DatabaseEntry(versionedSerializer.toBytes(val)));
                if(OperationStatus.SUCCESS != putStatus) {
                    String errorStr = "Put failed with " + putStatus + " for key"
                                      + BdbConvertData.writeAsciiString(stripedKey);
                    logger.error(errorStr);
                    throw new Exception(errorStr);
                }
                scanCount++;
            }
            if(scanCount % 1000000 == 0)
                logger.info("Reverted " + scanCount + "entries in "
                            + (System.currentTimeMillis() - startTime) / 1000 + " secs");
        }
        logger.info("Reverted " + scanCount + "entries and " + keyCount + " keys in "
                    + (System.currentTimeMillis() - startTime) / 1000 + " secs");
    }

    @Override
    public boolean areDuplicatesNeededForSrc() {
        return false;
    }

    @Override
    public boolean areDuplicatesNeededForDest() {
        return true;
    }
}
