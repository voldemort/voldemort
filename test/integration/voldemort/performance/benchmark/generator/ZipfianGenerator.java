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

import java.util.Random;

/**
 * A generator of a Zipfian distribution. It produces a sequence of items, such
 * that some items are more popular than others, according to a zipfian
 * distribution.
 * 
 * Note that the popular items will be clustered together, e.g. item 0 is the
 * most popular, item 1 the second most popular, and so on (or min is the most
 * popular, min+1 the next most popular, etc.)
 * 
 */
public class ZipfianGenerator extends IntegerGenerator {

    public static final double ZIPFIAN_CONSTANT = 0.99;

    private long items, base, countZeta;
    private double zipfianConstant, alpha, zetan, eta, theta, zeta2theta;
    private Random random;
    boolean allowItemCountDecrease = false;

    public ZipfianGenerator(long items) {
        this(0, items - 1);
    }

    public ZipfianGenerator(long min, long max) {
        this(min, max, ZIPFIAN_CONSTANT);
    }

    public ZipfianGenerator(long items, double zipfianConstant) {
        this(0, items - 1, zipfianConstant);
    }

    public ZipfianGenerator(long min, long max, double zipfianConstant) {
        this(min, max, zipfianConstant, zetaStatic(max - min + 1, zipfianConstant));
    }

    public ZipfianGenerator(long min, long max, double zipfianConstant, double zetan) {
        this.items = max - min + 1;
        this.base = min;
        this.zipfianConstant = zipfianConstant;
        this.random = new Random();
        this.theta = this.zipfianConstant;
        this.zeta2theta = zeta(2, theta);
        this.alpha = 1.0 / (1.0 - theta);
        this.zetan = zetan;
        this.countZeta = items;
        this.eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
        nextInt();
    }

    double zeta(long n, double theta) {
        countZeta = n;
        return zetaStatic(n, theta);
    }

    static double zetaStatic(long n, double theta) {
        return zetaStatic(0, n, theta, 0);
    }

    double zeta(long st, long n, double theta, double initialSum) {
        countZeta = n;
        return zetaStatic(st, n, theta, initialSum);
    }

    static double zetaStatic(long st, long n, double theta, double initialSum) {
        double sum = initialSum;
        for(long i = st; i < n; i++) {
            sum += 1 / (Math.pow(i + 1, theta));
        }
        return sum;
    }

    public int nextInt(int itemcount) {
        return (int) nextLong(itemcount);
    }

    public long nextLong(long itemCount) {
        // from "Quickly Generating Billion-Record Synthetic Databases"
        if(itemCount != countZeta) {

            synchronized(this) {
                if(itemCount > countZeta) {
                    zetan = zeta(countZeta, itemCount, theta, zetan);
                    eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
                } else if((itemCount < countZeta) && (allowItemCountDecrease)) {
                    zetan = zeta(itemCount, theta);
                    eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
                }
            }
        }
        double u = random.nextDouble();
        double uz = u * zetan;
        if(uz < 1.0) {
            return 0;
        }

        if(uz < 1.0 + Math.pow(0.5, theta)) {
            return 1;
        }

        long ret = base + (long) ((itemCount) * Math.pow(eta * u - eta + 1, alpha));
        setLastInt((int) ret);
        return ret;
    }

    @Override
    public int nextInt() {
        return (int) nextLong(items);
    }

    public long nextLong() {
        return nextLong(items);
    }
}
