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
import java.io.DataOutputStream;
import java.io.FileReader;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import voldemort.serialization.json.JsonReader;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

/**
 * @author jay
 * 
 */
public class ReadOnlyStoreBuilder {

    public static void main(String[] args) throws Exception {
        if(args.length != 6)
            Utils.croak("USAGE: java MmapStoreBuilder store_name input_file output_dir cluster.xml stores.xml sort_buffer_size");

        String storeName = args[0];
        String inputFile = args[1];
        String outputDir = args[2];
        String clusterFile = args[3];
        String storesFile = args[4];
        int sortBufferSize = Integer.parseInt(args[5]);

        BufferedReader reader = new BufferedReader(new FileReader(inputFile), 5000000);
        // needs type def
        JsonReader jsonReader = new JsonReader(reader);

        Pattern tab = Pattern.compile("\t");
        SortedMap<ByteKey, String> lines = new TreeMap<ByteKey, String>();
        for(String line = reader.readLine(); line != null; line = reader.readLine()) {
            String[] pieces = tab.split(line.trim());
            if(pieces.length != 2)
                Utils.croak("Invalid line: " + line);
            lines.put(new ByteKey(ByteUtils.md5(pieces[0].getBytes())), line);
        }
        reader.close();

        DataOutputStream index = null;// new DataOutputStream(new
                                      // BufferedOutputStream(new
                                      // FileOutputStream(outputFileBase +
                                      // ".index"), 1000000));
        DataOutputStream data = null;// new DataOutputStream(new
                                     // BufferedOutputStream(new
                                     // FileOutputStream(outputFileBase +
                                     // ".data"), 1000000));
        long position = 0;
        for(ByteKey key: lines.keySet()) {
            String line = lines.get(key);
            String[] pieces = tab.split(line);
            index.write(ByteUtils.md5(pieces[0].getBytes()));
            index.writeLong(position);
            System.out.println("Wrote " + pieces[0] + " " + pieces[0].getBytes() + " to "
                               + position);
            byte[] theValue = pieces[1].getBytes();
            data.writeInt(theValue.length);
            data.write(theValue);
            byte[] theKey = pieces[0].getBytes();
            data.writeInt(theKey.length);
            data.write(theKey);
            position += theKey.length + theValue.length + 4 + 4;
        }
        index.close();
        data.close();
    }

    private final static class ByteKey implements Comparable<ByteKey> {

        private final byte[] bytes;

        public ByteKey(byte[] bytes) {
            this.bytes = bytes;
        }

        public byte[] getBytes() {
            return this.bytes;
        }

        public boolean equals(Object o) {
            if(o == null)
                return false;
            if(o.getClass() != ByteKey.class)
                return false;
            return Arrays.equals(this.getBytes(), ((ByteKey) o).getBytes());
        }

        public int hashCode() {
            return Arrays.hashCode(getBytes());
        }

        public int compareTo(ByteKey k) {
            return ByteUtils.compare(this.getBytes(), k.getBytes());
        }
    }

}
