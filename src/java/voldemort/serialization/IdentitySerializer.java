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
 * A Serializer implementation that does nothing at all, just maps byte arrays to
 * identical byte arrays
 * 
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
