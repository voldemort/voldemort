package voldemort.client.protocol.pb;

import java.util.ArrayList;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.store.ErrorCodeMapper;
import voldemort.utils.ByteArray;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.protobuf.ByteString;

/**
 * Helper functions for serializing or deserializing client requests in protocol
 * buffers
 * 
 * @author jay
 * 
 */
public class ProtoUtils {

    public static VProto.Error.Builder encodeError(ErrorCodeMapper mapper, VoldemortException e) {
        return VProto.Error.newBuilder()
                           .setErrorCode(mapper.getCode(e))
                           .setErrorMessage(e.getMessage());
    }

    public static VProto.Versioned.Builder encodeVersioned(Versioned<byte[]> versioned) {
        return VProto.Versioned.newBuilder()
                               .setValue(ByteString.copyFrom(versioned.getValue()))
                               .setVersion(ProtoUtils.encodeClock(versioned.getVersion()));
    }

    public static Versioned<byte[]> decodeVersioned(VProto.Versioned versioned) {
        return new Versioned<byte[]>(versioned.getValue().toByteArray(),
                                     decodeClock(versioned.getVersion()));
    }

    public static List<Versioned<byte[]>> decodeVersions(List<VProto.Versioned> versioned) {
        List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>(versioned.size());
        for(VProto.Versioned v: versioned)
            values.add(decodeVersioned(v));
        return values;
    }

    public static VectorClock decodeClock(VProto.VectorClock encoded) {
        List<ClockEntry> entries = new ArrayList<ClockEntry>(encoded.getEntriesCount());
        for(VProto.ClockEntry entry: encoded.getEntriesList())
            entries.add(new ClockEntry((short) entry.getNodeId(), entry.getVersion()));
        return new VectorClock(entries, encoded.getTimestamp());
    }

    public static VProto.VectorClock.Builder encodeClock(Version version) {
        VectorClock clock = (VectorClock) version;
        VProto.VectorClock.Builder encoded = VProto.VectorClock.newBuilder();
        encoded.setTimestamp(clock.getTimestamp());
        for(ClockEntry entry: clock.getEntries())
            encoded.addEntries(VProto.ClockEntry.newBuilder()
                                                .setNodeId(entry.getNodeId())
                                                .setVersion(entry.getVersion()));
        return encoded;
    }

    public static ByteArray decodeBytes(ByteString string) {
        return new ByteArray(string.toByteArray());
    }

    public static ByteString encodeBytes(ByteArray array) {
        return ByteString.copyFrom(array.get());
    }

}
