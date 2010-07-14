/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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
 * the License. See accompanying LICENSE file.
 */

package voldemort.performance.benchmark.generator;

import voldemort.utils.FnvHashFunction;

/**
 * A generator of a Zipfian distribution. It produces a sequence of items, such
 * that some items are more popular than others, according to a Zipfian
 * distribution.
 * 
 * Unlike @ZipfianGenerator, this class scatters the "popular" items across the
 * item space. Use this, instead of @ZipfianGenerator, if you don't want the
 * head of the distribution (the popular items) clustered together.
 */
public class ScrambledZipfianGenerator extends IntegerGenerator {

    public static final double ZETAN = 52.93805640344461;
    public static final long ITEM_COUNT = 10000000000L;

    private ZipfianGenerator generator;
    private long min, max, itemCount;
    private FnvHashFunction hash;

    public ScrambledZipfianGenerator(long items) {
        this(0, items - 1);
    }

    public ScrambledZipfianGenerator(long min, long max) {
        this(min, max, ZipfianGenerator.ZIPFIAN_CONSTANT);
    }

    public ScrambledZipfianGenerator(long min, long max, double zipfianConstant) {
        this.min = min;
        this.max = max;
        this.itemCount = this.max - this.min + 1;
        generator = new ZipfianGenerator(0, ITEM_COUNT, zipfianConstant, ZETAN);
        hash = new FnvHashFunction();
    }

    @Override
    public int nextInt() {
        return (int) nextLong();
    }

    public long nextLong() {
        long ret = generator.nextLong();
        ret = min + hash.hash64(ret) % itemCount;
        setLastInt((int) ret);
        return ret;
    }

}
