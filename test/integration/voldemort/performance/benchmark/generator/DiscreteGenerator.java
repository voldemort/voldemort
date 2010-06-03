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
import java.util.Vector;

public class DiscreteGenerator extends Generator {

    private Vector<Pair> values;
    private Random random;
    private String lastValue;

    class Pair {

        public double weight;
        public String value;

        Pair(double weight, String value) {
            this.weight = weight;
            this.value = value;
        }
    }

    public DiscreteGenerator() {
        this.values = new Vector<Pair>();
        this.random = new Random();
        this.lastValue = null;
    }

    @Override
    public String nextString() {
        double sum = 0;

        for(Pair p: values) {
            sum += p.weight;
        }
        double val = random.nextDouble();
        for(Pair p: values) {
            if(val < p.weight / sum) {
                return p.value;
            }

            val -= p.weight / sum;
        }
        return null;
    }

    public int nextInt() throws Exception {
        throw new Exception("DiscreteGenerator does not support nextInt()");
    }

    @Override
    public String lastString() {
        if(lastValue == null) {
            lastValue = nextString();
        }
        return lastValue;
    }

    public void addValue(double weight, String value) {
        values.add(new Pair(weight, value));
    }

}
