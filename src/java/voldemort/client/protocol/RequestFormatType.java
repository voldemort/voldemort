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

package voldemort.client.protocol;

/**
 * An enumeration of request serialization types
 * 
 * @author jay
 * 
 */
public enum RequestFormatType {
    VOLDEMORT_V0("vp0", "voldemort-native-v0"),
    VOLDEMORT_V1("vp1", "voldemort-native-v1"),
    PROTOCOL_BUFFERS("pb0", "protocol-buffers-v0"),
    ADMIN_PROTOCOL_BUFFERS("ad1", "admin-v1");

    private final String code;
    private final String displayName;

    private RequestFormatType(String code, String display) {
        this.code = code;
        this.displayName = display;
    }

    public String getCode() {
        return code;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public static RequestFormatType fromCode(String code) {
        for(RequestFormatType type: RequestFormatType.values())
            if(type.getCode().equals(code))
                return type;
        throw new IllegalArgumentException("No request format '" + code + "' was found");
    }

}
