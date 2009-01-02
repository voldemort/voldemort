package voldemort.serialization;

/**
 * A Serializer implmentation that does nothing at all, just maps byte arrays to
 * identical byte arrays
 * 
 * @author jay
 * 
 */
public class IdentitySerializer implements Serializer<byte[]> {

    public byte[] toBytes(byte[] bytes) {
        return bytes;
    }

    public byte[] toObject(byte[] bytes) {
        return bytes;
    }

}
