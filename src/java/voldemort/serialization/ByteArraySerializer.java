package voldemort.serialization;

import voldemort.utils.ByteArray;

public final class ByteArraySerializer implements Serializer<ByteArray> {

    public byte[] toBytes(ByteArray object) {
        return object.get();
    }

    public ByteArray toObject(byte[] bytes) {
        return new ByteArray(bytes);
    }
}
