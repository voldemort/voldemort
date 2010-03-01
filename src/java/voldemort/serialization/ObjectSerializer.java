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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Transform java objects into bytes using java's built in serialization
 * mechanism
 * 
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
        } catch(IOException e) {
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
        } catch(IOException e) {
            throw new SerializationException(e);
        } catch(ClassNotFoundException c) {
            throw new SerializationException(c);
        }
    }

}
