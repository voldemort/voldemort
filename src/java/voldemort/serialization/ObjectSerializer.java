package voldemort.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Transform java objects into bytes using java's built in serialization
 * mechanism
 * 
 * @author jay
 * 
 * @param <T> The type of the object
 */
public class ObjectSerializer<T> implements Serializer<T> {

    /**
     * Transform the given object into an array of bytes
     * 
     * @param object The object to be serialized
     * @return The bytes created from serializing the object
     */
    public byte[] toBytes(T object) {
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(stream);
            out.writeObject(object);
            return stream.toByteArray();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Transform the given bytes into an object.
     * 
     * @param bytes The bytes to construct the object from
     * @return The object constructed
     */
    @SuppressWarnings("unchecked")
    public T toObject(byte[] bytes) {
        try {
            return (T) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
        } catch (IOException e) {
            throw new SerializationException(e);
        } catch (ClassNotFoundException c) {
            throw new SerializationException(c);
        }
    }

}
