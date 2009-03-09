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

package voldemort.performance;

import static voldemort.utils.Utils.croak;

import java.io.File;

import voldemort.client.StoreClient;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

public abstract class AbstractLoadTestHarness {

    public abstract StoreClient<String, String> getStore(Props propsA, Props propsB)
            throws Exception;

    public void run(String[] args) throws Exception {
        if(args.length != 4)
            croak("USAGE: java " + RemoteTest.class.getName()
                  + " numRequests numThreads properties_file1 properties_file2");

        int numRequests = Integer.parseInt(args[0]);
        int numThreads = Integer.parseInt(args[1]);
        Props propertiesA = new Props(new File(args[2]));
        Props propertiesB = new Props(new File(args[3]));

        System.out.println("Creating client: ");
        final StoreClient<String, String> client = getStore(propertiesA, propertiesB);

        PerformanceTest writeTest = new PerformanceTest() {

            @Override
            public void doOperation(int index) throws Exception {
                String iStr = Integer.toString(index);
                client.put(iStr, new Versioned<String>(iStr));
            }
        };
        System.out.println();
        System.out.println("WRITE TEST");
        writeTest.run(numRequests, numThreads);
        writeTest.printStats();

        System.out.println();

        PerformanceTest readTest = new PerformanceTest() {

            @Override
            public void doOperation(int index) throws Exception {
                String iStr = Integer.toString(index);
                String found = client.getValue(iStr);
                if(!iStr.equals(found))
                    System.err.println("Not equal: " + iStr + ", " + found);
            }
        };

        System.out.println("READ TEST");
        readTest.run(numRequests, numThreads);
        readTest.printStats();

        System.exit(0);
    }

}
