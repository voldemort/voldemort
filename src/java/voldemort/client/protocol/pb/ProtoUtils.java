/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.protocol.pb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

/**
 * Helper functions for serializing or deserializing client requests in protocol
 * buffers
 * 
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

    public static ByteString encodeTransform(byte[] transform) {
        return ByteString.copyFrom(transform);
    }

    public static void writeMessage(DataOutputStream output, Message message) throws IOException {
        /*
         * We don't use varints here because the c++ version of the protocol
         * buffer classes seem to be buggy requesting more data than necessary
         * from the underlying stream causing it to block forever
         */
        output.writeInt(message.getSerializedSize());
        CodedOutputStream codedOut = CodedOutputStream.newInstance(output);
        message.writeTo(codedOut);
        codedOut.flush();
    }

    public static void writeEndOfStream(DataOutputStream output) throws IOException {
        output.writeInt(-1);
    }

    public static <T extends Message.Builder> T readToBuilder(DataInputStream input, T builder)
            throws IOException {
        int size = input.readInt();
        CodedInputStream codedIn = CodedInputStream.newInstance(input);
        codedIn.pushLimit(size);
        builder.mergeFrom(codedIn);
        return builder;
    }
}
