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

package voldemort.store.readonly;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.commons.io.LineIterator;

import voldemort.serialization.StringSerializer;
import voldemort.utils.Utils;

/**
 * Perform an external sort on the lines of the file
 * 
 * 
 */
public class StringSorter {

    public static void main(String[] args) throws Exception {
        if(args.length != 3)
            Utils.croak("USAGE: java StringSorter inputfile internal_sort_size num_threads");
        String input = args[0];
        int internalSortSize = Integer.parseInt(args[1]);
        int numThreads = Integer.parseInt(args[2]);

        ExternalSorter<String> sorter = new ExternalSorter<String>(new StringSerializer(),
                                                                   internalSortSize,
                                                                   numThreads);
        @SuppressWarnings("unchecked")
        Iterator<String> it = new LineIterator(new BufferedReader(new FileReader(input),
                                                                  10 * 1024 * 1024));

        String seperator = Utils.NEWLINE;
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out),
                                                   10 * 1024 * 1024);
        for(String line: sorter.sorted(it)) {
            writer.write(line);
            writer.write(seperator);
        }
    }

}
