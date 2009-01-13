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

package voldemort.store;

import com.google.common.base.Objects;

/**
 * Wrapper class that handles arrays as keys properly.
 * 
 * @author jay
 * 
 */
public class KeyWrapper {

    private final Object k;

    public KeyWrapper(Object k) {
        this.k = k;
    }

    public Object get() {
        return k;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(!(o instanceof KeyWrapper))
            return false;

        KeyWrapper key = (KeyWrapper) o;
        return Objects.deepEquals(k, key.get());
    }

    @Override
    public int hashCode() {
        return Objects.deepHashCode(k);
    }

    @Override
    public String toString() {
        return "Key(" + k.toString() + ")";
    }

}