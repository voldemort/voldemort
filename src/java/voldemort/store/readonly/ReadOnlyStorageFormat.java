/*
 * Copyright 2010 LinkedIn, Inc
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
package voldemort.store.readonly;

/**
 * An enumeration of read-only stores formats
 */
public enum ReadOnlyStorageFormat {
    READONLY_V0("ro0", "node-chunks-v0"),
    READONLY_V1("ro1", "partition-chunks-v1"),
    READONLY_V2("ro2", "replica-chunks-with-keys-v2");

    private final String code;
    private final String displayName;

    private ReadOnlyStorageFormat(String code, String display) {
        this.code = code;
        this.displayName = display;
    }

    public String getCode() {
        return code;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public static ReadOnlyStorageFormat fromCode(String code) {
        for(ReadOnlyStorageFormat type: ReadOnlyStorageFormat.values())
            if(type.getCode().equals(code))
                return type;
        throw new IllegalArgumentException("No request format '" + code + "' was found");
    }

    @Override
    public String toString() {
        return code;
    }

}
