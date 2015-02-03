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

package voldemort.store.readonly.mr.utils;

import java.util.Arrays;

import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.utils.Utils;

public class JsonSchema {

    private final JsonTypeDefinition key;
    private final JsonTypeDefinition value;

    public JsonSchema(JsonTypeDefinition key, JsonTypeDefinition value) {
        super();
        this.key = Utils.notNull(key);
        this.value = Utils.notNull(value);
    }

    public JsonTypeDefinition getKeyType() {
        return key;
    }

    public JsonTypeDefinition getValueType() {
        return value;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[] { key, value });
    }

    @Override
    public boolean equals(Object o) {
        if(o == null)
            return false;
        if(o == this)
            return true;
        if(o.getClass() != JsonSchema.class)
            return false;
        JsonSchema s = (JsonSchema) o;
        return getKeyType().equals(s.getKeyType()) && getValueType().equals(s.getValueType());
    }

    @Override
    public String toString() {
        return key.toString() + " => " + value.toString();
    }

}
