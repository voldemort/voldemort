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

import junit.framework.TestCase;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.ByteUtils;

public class CheckSumTests extends TestCase {

    public void testCheckSum() {
        CheckSum instance = CheckSum.getInstance("blah");
        assertNull(instance);

        CheckSum instance2 = CheckSum.getInstance("md5");
        assertNotNull(instance2);

        CheckSum instance3 = CheckSum.getInstance("crc32");
        assertNotNull(instance3);

        CheckSum instance4 = CheckSum.getInstance("adler32");
        assertNotNull(instance4);

        byte[] emptyBytes = new byte[0];
        instance2.update(emptyBytes);
        assertNotNull(instance2.getCheckSum());
        assertEquals(instance2.getCheckSum().length, CheckSum.checkSumLength(CheckSumType.MD5));

        instance3.update(emptyBytes);
        assertNotNull(instance3.getCheckSum());
        assertEquals(instance3.getCheckSum().length, CheckSum.checkSumLength(CheckSumType.CRC32));

        instance4.update(emptyBytes);
        assertNotNull(instance4.getCheckSum());
        assertEquals(instance4.getCheckSum().length, CheckSum.checkSumLength(CheckSumType.ADLER32));

    }

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
                if(fs2.isDirectory())
                    return fs1.isDirectory() ? 0 : 1;

                String f1 = fs1.getName(), f2 = fs2.getName();

                // All checksum files given priority
                if(f1.endsWith("metadata"))
                    return -1;
                if(f2.endsWith("metadata"))
                    return 1;

                // if both same, lexicographically
                if((f1.endsWith(".index") && f2.endsWith(".index"))
                   || (f1.endsWith(".data") && f2.endsWith(".data"))) {
                    return f1.compareToIgnoreCase(f2);
                }

                if(f1.endsWith(".index")) {
                    return 1;
                } else {
                    return -1;
                }
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
