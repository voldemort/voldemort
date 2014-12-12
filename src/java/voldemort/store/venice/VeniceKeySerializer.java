package voldemort.store.venice;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import voldemort.utils.ByteArray;

/**
 * A simple class which writes ByteArray objects into byte[]
 * Implements classes required for Kafka Serialization
 */
public class VeniceKeySerializer implements Encoder<ByteArray>, Decoder<ByteArray> {

    public VeniceKeySerializer(VerifiableProperties properties) {

    }

    @Override
    public ByteArray fromBytes(byte[] bytes) {
        return new ByteArray(bytes);
    }

    @Override
    public byte[] toBytes(ByteArray byteArray) {
        return byteArray.get();
    }
}
