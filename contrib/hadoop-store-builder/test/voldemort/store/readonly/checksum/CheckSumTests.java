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

package voldemort.store.readonly.checksum;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.ByteUtils;

public class CheckSumTests {

    /**
     * Calculates the checksum of checksums of individual files.
     * 
     * @param files The files whose bytes to read
     * @param checkSumType
     * @return bytes
     */
    public static byte[] calculateCheckSum(File[] files, CheckSumType checkSumType)
            throws Exception {
        CheckSum checkSumGenerator = CheckSum.getInstance(checkSumType);
        CheckSum fileCheckSumGenerator = CheckSum.getInstance(checkSumType);

        int bufferSize = 64 * 1024;
        byte[] buffer = new byte[bufferSize];

        Arrays.sort(files, new Comparator<File>() {

            public int compare(File fs1, File fs2) {
                // directories before files
                if(fs1.isDirectory())
                    return fs2.isDirectory() ? 0 : -1;
                // move checksum files to the top to determine the algorithm
                // used
                else if(fs1.getName().endsWith("checkSum.txt"))
                    return -1;
                else if(fs2.getName().endsWith("checkSum.txt"))
                    return 1;
                // index files after all other files
                else if(fs1.getName().endsWith(".index"))
                    return fs2.getName().endsWith(".index") ? 0 : 1;
                // everything else is equivalent
                else
                    return 0;
            }
        });

        for(File file: files) {
            if(file.isFile() && !file.getName().startsWith(".")
               && !file.getName().contains("checkSum.txt")) {
                DataInputStream is;
                try {
                    is = new DataInputStream(new FileInputStream(file));
                } catch(FileNotFoundException e) {
                    continue;
                }

                try {
                    while(true) {
                        int read = is.read(buffer);
                        if(read < 0)
                            break;
                        else if(read < bufferSize) {
                            buffer = ByteUtils.copy(buffer, 0, read);
                        }
                        fileCheckSumGenerator.update(buffer);
                    }
                    is.close();
                } catch(IOException e) {
                    break;
                }
                checkSumGenerator.update(fileCheckSumGenerator.getCheckSum());
            }
        }
        return checkSumGenerator.getCheckSum();

    }

}
