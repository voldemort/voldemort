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

import java.io.BufferedReader;
import java.io.FileReader;

import voldemort.serialization.json.JsonReader;
import voldemort.utils.Utils;

/**
 * @author jay
 * 
 */
public class ReadJson {

    public static void main(String[] args) throws Exception {
        if(args.length != 1)
            Utils.croak("USAGE: java ReadJson filename");
        long start = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new FileReader(args[0]), 1000000);
        JsonReader jReader = new JsonReader(reader);
        int count;
        for(count = 0; jReader.hasMore(); count++) {
            jReader.read();
            if(count % 1000000 == 0)
                System.out.println(count);
        }
        System.out.println((System.currentTimeMillis() - start) / (double) count + " ms/object");
    }

}
