package voldemort.serialization;

import voldemort.utils.ByteUtils;

/**
 * A Serializer that serializes strings
 * 
 * @author jay
 * 
 */
public class StringSerializer implements Serializer<String> {

    private String encoding;

    public StringSerializer() {
        this("UTF-8");
    }

    public StringSerializer(String encoding) {
        this.encoding = encoding;
    }

    public byte[] toBytes(String string) {
        if(string == null)
            return null;
        return ByteUtils.getBytes(string, encoding);
    }

    public String toObject(byte[] bytes) {
        if(bytes == null)
            return null;
        return ByteUtils.getString(bytes, encoding);
    }

}
