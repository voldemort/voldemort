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

package voldemort.utils;

import java.util.Iterator;

import com.google.common.collect.Iterables;

/**
 * An iterable that always produces the given iterator
 * 
 * 
 */
public class DefaultIterable<V> implements Iterable<V> {

    private final Iterator<V> iterator;

    public DefaultIterable(Iterator<V> iterator) {
        this.iterator = iterator;
    }

    public Iterator<V> iterator() {
        return this.iterator;
    }

    @Override
    public String toString() {
        return Iterables.toString(this);
    }
}
