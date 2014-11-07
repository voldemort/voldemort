package voldemort.store.venice;

import voldemort.store.routed.Pipeline;

/**
 * Class which stores the components of VeniceMessage, and is the format specified in the Kafka Serializer
 */
public class VeniceMessage {

    public static final byte DEFAULT_MAGIC_BYTE = 22;
    public static final byte DEFAULT_SCHEMA_VERSION = -1;

    public static final byte FULL_OPERATION_BYTE = 0;
    public static final byte PARTIAL_OPERATION_BYTE = 1;
    public static final byte[] FULL_OPERATION_BYTEARRAY = { FULL_OPERATION_BYTE };
    public static final byte[] PARTIAL_OPERATION_BYTEARRAY = { PARTIAL_OPERATION_BYTE };

    private byte magicByte;
    private int schemaVersion;

    private OperationType operationType;
    private byte[] payload;

    // TODO: find best data type for timestamp
    private Object timestamp;

    // A message without a payload (used for deletes)
    public VeniceMessage(OperationType type) {
        this.operationType = (type == OperationType.PUT) ? OperationType.ERROR : type;
        this.magicByte = DEFAULT_MAGIC_BYTE;
        this.timestamp = null;
        this.schemaVersion = DEFAULT_SCHEMA_VERSION;
        this.payload = new byte[0];
    }

    public VeniceMessage(OperationType type, byte[] payload) {
        this(type, payload, DEFAULT_SCHEMA_VERSION);
    }

    public VeniceMessage(OperationType type, byte[] payload, int schemaVersion) {
        this.magicByte = DEFAULT_MAGIC_BYTE;
        this.timestamp = null;
        this.schemaVersion = schemaVersion;
        this.operationType = type;
        this.payload = payload;
    }

    public byte getMagicByte() {
        return magicByte;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String toString() {
        return operationType.toString() + " " + payload.toString();
    }

    public Object getTimestamp() {
        return timestamp;
    }

}
