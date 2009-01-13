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

package voldemort;

public class TestArrayCopy {

    /**
     * @param args
     */
    public static void main(String[] args) {

        int iterations = 1000000000;
        int size = 20;

        byte[] source = new byte[size];
        byte[] dest = new byte[size];
        for(int i = 0; i < size; i++)
            source[i] = (byte) i;

        // test arraycopy
        long start = System.nanoTime();
        for(int i = 0; i < iterations; i++)
            System.arraycopy(source, 0, dest, 0, size);
        long ellapsed = System.nanoTime() - start;
        System.out.println("System.arraycopy(): " + (ellapsed / (double) iterations));

        // test for loop
        start = System.nanoTime();
        for(int i = 0; i < iterations; i++)
            for(int j = 0; j < size; j++)
                dest[j] = source[j];
        ellapsed = System.nanoTime() - start;
        System.out.println("for loop: " + (ellapsed / (double) iterations));

    }

}
