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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;

import voldemort.utils.ByteUtils;

/**
 * Tests for the HDFS-based fetcher
 * 
 * 
 */
public class HdfsFetcherTest extends TestCase {

    public void testFetch() throws IOException {
        File testFile = File.createTempFile("test", ".dat");
        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = fetcher.fetch(testFile.getAbsolutePath());
        InputStream orig = new FileInputStream(testFile);
        byte[] origBytes = IOUtils.toByteArray(orig);
        InputStream fetched = new FileInputStream(fetchedFile);
        byte[] fetchedBytes = IOUtils.toByteArray(fetched);
        assertTrue("Fetched bytes not equal to original bytes.",
                   0 == ByteUtils.compare(origBytes, fetchedBytes));
    }

}
