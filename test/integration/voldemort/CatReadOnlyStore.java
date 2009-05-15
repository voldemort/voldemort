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

import java.io.EOFException;
import java.io.File;
import java.io.RandomAccessFile;

import voldemort.serialization.Serializer;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

public class CatReadOnlyStore {

    public static void main(String[] args) throws Exception {
        if(args.length != 1)
            Utils.croak("USAGE: java " + CatReadOnlyStore.class.getName() + " directory");
        File dir = new File(args[0]);
        Serializer<Object> serializer = new JsonTypeSerializer(JsonTypeDefinition.fromJson("'string'"),
                                                               true);
        for(int chunk = 0;; chunk++) {
            File indexFile = new File(dir, chunk + ".index");
            File dataFile = new File(dir, chunk + ".data");
            if(!indexFile.exists() || !dataFile.exists())
                break;
            RandomAccessFile index = new RandomAccessFile(indexFile, "r");
            RandomAccessFile data = new RandomAccessFile(dataFile, "r");

            System.out.println("Chunk " + chunk);
            byte[] keyMd5 = new byte[16];
            long position = -1;
            try {
                while(true) {
                    index.readFully(keyMd5);
                    position = index.readInt();

                    data.seek(position);
                    int size = data.readInt();
                    byte[] value = new byte[size];
                    data.readFully(value);
                    System.out.println(ByteUtils.toHexString(keyMd5) + "\t=>\t"
                                       + serializer.toObject(value).toString());
                }
            } catch(EOFException e) {
                // no biggie
            }
            System.out.println();
        }
    }
}
