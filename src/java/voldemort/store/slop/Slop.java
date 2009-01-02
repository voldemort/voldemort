package voldemort.store.slop;

import java.util.Arrays;
import java.util.Date;

import voldemort.utils.ByteUtils;

import com.google.common.base.Objects;

/**
 * Represents an undelivered operation to a store.
 * 
 * @author jay
 * 
 */
public class Slop {

    private static final byte[] spacer = new byte[] { (byte) 0 };

    public enum Operation {
        GET((byte) 0),
        PUT((byte) 1),
        DELETE((byte) 2);

        private final byte opCode;

        private Operation(byte opCode) {
            this.opCode = opCode;
        }

        public byte getOpCode() {
            return opCode;
        }
    };

    final private byte[] key;
    final private byte[] value;
    final private String storeName;
    final private int nodeId;
    final private Date arrived;
    final private Operation operation;

    public Slop(String storeName,
                Operation operation,
                byte[] key,
                byte[] value,
                int nodeId,
                Date arrived) {
        this.operation = Objects.nonNull(operation);
        this.storeName = Objects.nonNull(storeName);
        this.key = Objects.nonNull(key);
        this.value = value;
        this.nodeId = nodeId;
        this.arrived = Objects.nonNull(arrived);
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public int getNodeId() {
        return this.nodeId;
    }

    public Date getArrived() {
        return arrived;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getStoreName() {
        return storeName;
    }

    public byte[] makeKey() {
        byte[] storeName = ByteUtils.getBytes(getStoreName(), "UTF-8");
        byte[] opCode = new byte[] { operation.getOpCode() };
        return ByteUtils.cat(opCode, spacer, storeName, spacer, key);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        else if (!obj.getClass().equals(Slop.class))
            return false;

        Slop slop = (Slop) obj;

        return operation == slop.getOperation() && Objects.equal(storeName, getStoreName())
               && Objects.deepEquals(key, slop.getKey())
               && Objects.deepEquals(value, slop.getValue()) && nodeId == slop.getNodeId()
               && Objects.equal(arrived, slop.getArrived());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(storeName, operation, nodeId, arrived) + Arrays.hashCode(key)
               + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "Slop(storeName = " + storeName + ", operation = " + operation + ", key = " + key
               + ", value = " + value + ", nodeId = " + nodeId + ", arrived" + arrived + ")";
    }

}
