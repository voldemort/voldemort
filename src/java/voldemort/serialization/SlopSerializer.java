package voldemort.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

import voldemort.store.slop.Slop;
import voldemort.utils.ByteUtils;

/**
 * A Serializer for writing Slops
 * 
 * @author jay
 * 
 */
public class SlopSerializer implements Serializer<Slop> {

    public byte[] toBytes(Slop slop) {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(byteOutput);
        try {
            data.writeUTF(slop.getStoreName());
            data.writeUTF(slop.getOperation().toString());
            data.writeInt(slop.getKey().length);
            data.write(slop.getKey());
            if (slop.getValue() == null) {
                data.writeInt(-1);
            } else {
                data.writeInt(slop.getValue().length);
                data.write(slop.getValue());
            }
            data.writeInt(slop.getNodeId());
            data.writeLong(slop.getArrived().getTime());
            return byteOutput.toByteArray();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    public Slop toObject(byte[] bytes) {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            String storeName = input.readUTF();
            Slop.Operation op = Slop.Operation.valueOf(input.readUTF());
            int keySize = input.readInt();
            byte[] key = new byte[keySize];
            ByteUtils.read(input, key);
            int size = input.readInt();
            byte[] value = null;
            if (size >= 0) {
                value = new byte[size];
                ByteUtils.read(input, value);
            }
            int nodeId = input.readInt();
            Date arrived = new Date(input.readLong());
            return new Slop(storeName, op, key, value, nodeId, arrived);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

}
