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

package voldemort.performance.benchmark.generator;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FileStringGenerator extends Generator {

    private List<String> keys = null;
    private final AtomicInteger index;
    private String lastString;

    public FileStringGenerator(int start, List<String> keys) {
        this.index = new AtomicInteger(start);
        this.keys = keys;
    }

    @Override
    public String nextString() {
        String key = keys.get(index.getAndIncrement() % keys.size());
        lastString = key;
        return key;
    }

    @Override
    public String lastString() {
        return lastString;
    }
}
