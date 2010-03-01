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

package voldemort.serialization.json;

import voldemort.serialization.SerializationException;

/**
 * An enum of JsonTypes (for convenience)
 * 
 * 
 */
public enum JsonTypes {
    BOOLEAN("boolean"),
    STRING("string"),
    INT8("int8"),
    INT16("int16"),
    INT32("int32"),
    INT64("int64"),
    FLOAT32("float32"),
    FLOAT64("float64"),
    BYTES("bytes"),
    DATE("date");

    private final String display;

    private JsonTypes(String display) {
        this.display = display;
    }

    public String toDisplay() {
        return display;
    }

    public static JsonTypes fromDisplay(String name) {
        for(JsonTypes t: JsonTypes.values())
            if(t.toDisplay().equals(name))
                return t;
        throw new SerializationException(name + " is not a valid display for any SimpleType.");
    }

}
