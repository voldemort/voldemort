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

/**
 * A Serializer that serializes strings
 * 
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
