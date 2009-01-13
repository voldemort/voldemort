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

package voldemort.partition;

import java.io.BufferedReader;
import java.io.FileReader;

import voldemort.utils.FnvHashFunction;

public class FnvHashFunctionTester {

    public static void main(String[] argv) throws Exception {
        if(argv.length != 1) {
            System.err.println("USAGE: java voldemort.partition.FnvHashFunctionTester filename");
            System.exit(1);
        }

        FnvHashFunction hash = new FnvHashFunction();
        BufferedReader reader = new BufferedReader(new FileReader(argv[0]));
        while(true) {
            String line = reader.readLine();
            if(line == null)
                break;
            System.out.println(hash.hash(line.trim().getBytes()));
        }

        reader.close();

    }

}
