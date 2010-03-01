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

/**
 * Map objects to byte arrays and back again
 * 
 * 
 * @param <T> The type of the object that is mapped by this serializer
 */
public interface Serializer<T> {

    /**
     * Construct an array of bytes from the given object
     * 
     * @param object The object
     * @return The bytes taken from the object
     */
    public byte[] toBytes(T object);

    /**
     * Create an object from an array of bytes
     * 
     * @param bytes An array of bytes with the objects data
     * @return A java object serialzed from the bytes
     */
    public T toObject(byte[] bytes);

}
