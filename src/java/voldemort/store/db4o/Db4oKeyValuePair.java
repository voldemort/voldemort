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

    private Key key_;
    private Value value_;

    public Db4oKeyValuePair(Key key, Value value) {
        this.key_ = key;
        this.value_ = value;
    }

    public void setKey(Key key) {
        this.key_ = key;
    }

    public Key getKey() {
        return key_;
    }

    public void setValue(Value value) {
        this.value_ = value;
    }

    public Value getValue() {
        return value_;
    }

    public Pair<Key, Value> toVoldemortPair() {
        return Pair.create(getKey(), getValue());
    }

}
