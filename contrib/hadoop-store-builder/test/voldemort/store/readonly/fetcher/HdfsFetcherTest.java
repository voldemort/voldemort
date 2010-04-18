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

package voldemort.store.readonly.fetcher;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.utils.ByteUtils;

/**
 * Tests for the HDFS-based fetcher
 * 
 * 
 */
public class HdfsFetcherTest extends TestCase {

    public void testFetch() throws Exception {
        File testDirectory = TestUtils.createTempDir();

        File testFile = File.createTempFile("test", ".dat", testDirectory);
        testFile.createNewFile();

        // No checksum file - return correctly
        // Required for backward compatibility with existing hadoop stores
        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = fetcher.fetch(testDirectory.getAbsolutePath(), "storeName");
        assertNotNull(fetchedFile);

        // Add checksum file with incorrect checksum
        File checkSumFile = new File(testDirectory, "checkSum.txt");
        checkSumFile.createNewFile();
        fetchedFile = fetcher.fetch(testDirectory.getAbsolutePath(), "storeName");
        assertNull(fetchedFile);

        // Add correct checksum file
        byte[] fileBytes = TestUtils.readBytes(testDirectory.listFiles());

        checkSumFile = new File(testDirectory, "checkSum.txt");
        DataOutputStream os = new DataOutputStream(new FileOutputStream(checkSumFile));
        os.write(ByteUtils.md5(fileBytes));
        os.close();

        fetchedFile = fetcher.fetch(testDirectory.getAbsolutePath(), "storeName");
        assertNotNull(fetchedFile);

    }

}
