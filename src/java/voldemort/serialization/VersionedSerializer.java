package voldemort.serialization;

import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * A Serializer that removes the Versioned wrapper and delegates to a user-supplied
 * serializer to deal with the remaining bytes
 * 
 * @author jay
 *
 * @param <T> The Versioned type
 */
public class VersionedSerializer<T> implements Serializer<Versioned<T>> {
	
	private final Serializer<T> innerSerializer;

	public VersionedSerializer(Serializer<T> innerSerializer) {
		this.innerSerializer = innerSerializer;
	}

	public byte[] toBytes(Versioned<T> versioned) {
		byte[] versionBytes = null;
		if(versioned.getVersion() == null)
			versionBytes = new byte[]{-1};
		else
			versionBytes = ((VectorClock) versioned.getVersion()).toBytes();
		byte[] objectBytes = innerSerializer.toBytes(versioned.getValue());
		return ByteUtils.cat(versionBytes, objectBytes);
	}

	public Versioned<T> toObject(byte[] bytes) {
		VectorClock clock = null;
		int size = 1;
		if(bytes[0] >= 0) {
			clock = new VectorClock(bytes);
			size = clock.sizeInBytes();
		}
		T t = innerSerializer.toObject(ByteUtils.copy(bytes, size, bytes.length));
		return new Versioned<T>(t, clock);
	}

}
