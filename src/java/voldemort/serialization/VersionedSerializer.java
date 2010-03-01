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

package voldemort.serialization;

import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A Serializer that removes the Versioned wrapper and delegates to a
 * user-supplied serializer to deal with the remaining bytes
 * 
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
            versionBytes = new byte[] { -1 };
        else
            versionBytes = ((VectorClock) versioned.getVersion()).toBytes();
        byte[] objectBytes = innerSerializer.toBytes(versioned.getValue());
        return ByteUtils.cat(versionBytes, objectBytes);
    }

    public Versioned<T> toObject(byte[] bytes) {
        VectorClock vectorClock = getVectorClock(bytes);

        int size = 1;
        if(vectorClock != null)
            size = vectorClock.sizeInBytes();

        T t = innerSerializer.toObject(ByteUtils.copy(bytes, size, bytes.length));
        return new Versioned<T>(t, vectorClock);
    }

    public Version getVersion(byte[] bytes) {
        return getVectorClock(bytes);
    }

    private VectorClock getVectorClock(byte[] bytes) {
        if(bytes[0] >= 0)
            return new VectorClock(bytes);
        return null;
    }

}
