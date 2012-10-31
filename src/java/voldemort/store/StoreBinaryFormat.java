package voldemort.store;

import java.util.ArrayList;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/*-
 *  Defines a generic on-disk binary data format for versioned voldemort data
 * -----------------------------------------
 *    FORMAT_VERSION                       : 1 byte
 *    Versioned value (repeating) {
 *        Clock (variable length) {
 *            NUM_CLOCK_ENTRIES            : 2 bytes (short)
 *            VERSION_SIZE                 : 1 byte
 *            Server clock (repeating) {
 *                NODE_ID                  : 2 bytes (short)
 *                VERSION                  : VERSION_SIZE bytes
 *            }
 *        }
 *        Value (variable length) {
 *             VALUE_SIZE                  : 4 bytes (int)
 *             VALUE_BYTES                 : VALUE_SIZE bytes
 *        }
 *    }     
 * -----------------------------------------
 */
public class StoreBinaryFormat {

    /* In the future we can use this to handle format changes */
    private static final byte VERSION = 0;

    private static final int PARTITIONID_PREFIX_SIZE = ByteUtils.SIZE_OF_SHORT;

    public static byte[] toByteArray(List<Versioned<byte[]>> values) {
        int size = 1;
        for(Versioned<byte[]> v: values) {
            size += ((VectorClock) v.getVersion()).sizeInBytes();
            size += ByteUtils.SIZE_OF_INT;
            size += v.getValue().length;
        }
        byte[] bytes = new byte[size];
        int pos = 1;
        bytes[0] = VERSION;
        for(Versioned<byte[]> v: values) {
            pos += ((VectorClock) v.getVersion()).toBytes(bytes, pos);
            int len = v.getValue().length;
            ByteUtils.writeInt(bytes, len, pos);
            pos += ByteUtils.SIZE_OF_INT;
            System.arraycopy(v.getValue(), 0, bytes, pos, len);
            pos += len;
        }
        if(pos != bytes.length)
            throw new VoldemortException((bytes.length - pos)
                                         + " straggling bytes found in value (this should not be possible)!");
        return bytes;
    }

    public static List<Versioned<byte[]>> fromByteArray(byte[] bytes) {
        if(bytes.length < 1)
            throw new VoldemortException("Invalid value length: " + bytes.length);
        if(bytes[0] != VERSION)
            throw new VoldemortException("Unexpected version number in value: " + bytes[0]);
        int pos = 1;
        List<Versioned<byte[]>> vals = new ArrayList<Versioned<byte[]>>(2);
        while(pos < bytes.length) {
            VectorClock clock = new VectorClock(bytes, pos);
            pos += clock.sizeInBytes();
            int valueSize = ByteUtils.readInt(bytes, pos);
            pos += ByteUtils.SIZE_OF_INT;
            byte[] val = new byte[valueSize];
            System.arraycopy(bytes, pos, val, 0, valueSize);
            pos += valueSize;
            vals.add(Versioned.value(val, clock));
        }
        if(pos != bytes.length)
            throw new VoldemortException((bytes.length - pos)
                                         + " straggling bytes found in value (this should not be possible)!");
        return vals;
    }

    public static byte[] makePrefixedKey(byte[] key, int partitionId) {
        byte[] prefixedKey = new byte[PARTITIONID_PREFIX_SIZE + key.length];
        ByteUtils.writeUnsignedShort(prefixedKey, partitionId, 0);
        System.arraycopy(key, 0, prefixedKey, PARTITIONID_PREFIX_SIZE, key.length);
        return prefixedKey;
    }

    public static byte[] makePartitionKey(int partitionId) {
        byte[] partitionKey = new byte[PARTITIONID_PREFIX_SIZE];
        ByteUtils.writeUnsignedShort(partitionKey, partitionId, 0);
        return partitionKey;
    }

    public static int extractPartition(byte[] prefixedKeyArray) {
        return ByteUtils.readUnsignedShort(prefixedKeyArray, 0);
    }

    public static byte[] extractKey(byte[] prefixedKeyArray) {
        byte[] key = new byte[prefixedKeyArray.length - PARTITIONID_PREFIX_SIZE];
        System.arraycopy(prefixedKeyArray, PARTITIONID_PREFIX_SIZE, key, 0, key.length);
        return key;
    }
}