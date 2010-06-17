/*
 * Copyright 2010 Versant Corporation
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

package voldemort.store.db4o;

import voldemort.utils.Pair;

public class Db4oKeyValuePair<Key, Value> {

    protected Key key;
    protected Value value;

    public Db4oKeyValuePair(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public void setKey(Key key) {
        this.key = key;
    }

    public Key getKey() {
        return key;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    public Value getValue() {
        return value;
    }

    public Pair<Key, Value> toVoldemortPair() {
        return Pair.create(getKey(), getValue());
    }

}
