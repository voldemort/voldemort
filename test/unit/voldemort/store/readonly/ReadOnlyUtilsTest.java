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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

/**
 * Tests for the HDFS-based fetcher
 * 
 * 
 */
public class ReadOnlyUtilsTest extends TestCase {

    public void testMinIntegerBug() {
        byte[] keyBytes = new byte[4];
        ByteUtils.writeInt(keyBytes, Integer.MIN_VALUE, 0);
        assertEquals(0, ReadOnlyUtils.chunk(keyBytes, 15));
    }

    public void testVersionParsing() throws IOException {
        File tempParentDir = TestUtils.createTempDir();

        File testFile = new File(tempParentDir, "blahDir");
        Utils.mkdirs(testFile);
        assertFalse(ReadOnlyUtils.checkVersionDirName(testFile));
        assertEquals(ReadOnlyUtils.getVersionId(testFile), -1);

        File testFile2 = new File(tempParentDir, "blahFile");
        testFile2.createNewFile();
        assertFalse(ReadOnlyUtils.checkVersionDirName(testFile2));
        assertEquals(ReadOnlyUtils.getVersionId(testFile2), -1);

        File testFile3 = new File(tempParentDir, "version-23");
        Utils.mkdirs(testFile3);
        assertTrue(ReadOnlyUtils.checkVersionDirName(testFile3));
        assertEquals(ReadOnlyUtils.getVersionId(testFile3), 23);

        File testFile4 = new File(tempParentDir, "version-23.bak");
        Utils.mkdirs(testFile4);
        assertFalse(ReadOnlyUtils.checkVersionDirName(testFile4));
        assertEquals(ReadOnlyUtils.getVersionId(testFile4), -1);

        File latestSymLink = new File(tempParentDir, "latest");
        Utils.symlink(testFile.getAbsolutePath(), latestSymLink.getAbsolutePath());
        assertFalse(ReadOnlyUtils.checkVersionDirName(latestSymLink));
        assertEquals(ReadOnlyUtils.getVersionId(latestSymLink), -1);

        assertEquals(ReadOnlyUtils.getVersionDirs(tempParentDir).length, 1);
    }

    private void generateVersionDirs(final File parentDir, final String... versions) {
        File versionDir = null;
        for(String version: versions) {
            versionDir = new File(parentDir, "version-" + version);
            Utils.mkdirs(versionDir);
        }
    }

    public void testFindKthVersionedDir() {
        File tempParentDir = TestUtils.createTempDir();
        generateVersionDirs(tempParentDir, "0");
        assertNull(ReadOnlyUtils.findKthVersionedDir(ReadOnlyUtils.getVersionDirs(tempParentDir),
                                                     -1,
                                                     100));
        assertNull(ReadOnlyUtils.findKthVersionedDir(ReadOnlyUtils.getVersionDirs(tempParentDir),
                                                     0,
                                                     1));

        File[] returnedFiles = ReadOnlyUtils.findKthVersionedDir(ReadOnlyUtils.getVersionDirs(tempParentDir),
                                                                 0,
                                                                 0);
        assertEquals(returnedFiles.length, 1);
        assertEquals(returnedFiles[0], new File(tempParentDir, "version-0"));

        tempParentDir = TestUtils.createTempDir();
        String[] versions = { "100", "10", "200", "20", "250", "300", "6", "6a" };
        generateVersionDirs(tempParentDir, versions);

        returnedFiles = ReadOnlyUtils.getVersionDirs(tempParentDir);
        assertEquals(returnedFiles.length, 7);

        File[] returnedFiles2 = ReadOnlyUtils.findKthVersionedDir(returnedFiles, 5, 6);
        assertEquals(returnedFiles2.length, 2);
        assertEquals(returnedFiles2[0], new File(tempParentDir, "version-250"));
        assertEquals(returnedFiles2[1], new File(tempParentDir, "version-300"));

        returnedFiles2 = ReadOnlyUtils.findKthVersionedDir(returnedFiles, 0, 3);
        assertEquals(returnedFiles2.length, 4);
        assertEquals(returnedFiles2[0], new File(tempParentDir, "version-6"));
        assertEquals(returnedFiles2[1], new File(tempParentDir, "version-10"));
        assertEquals(returnedFiles2[2], new File(tempParentDir, "version-20"));
        assertEquals(returnedFiles2[3], new File(tempParentDir, "version-100"));

        returnedFiles2 = ReadOnlyUtils.findKthVersionedDir(returnedFiles, 0, 0);
        assertEquals(returnedFiles2.length, 1);
        assertEquals(returnedFiles2[0], new File(tempParentDir, "version-6"));

        returnedFiles2 = ReadOnlyUtils.findKthVersionedDir(returnedFiles, 6, 6);
        assertEquals(returnedFiles2.length, 1);
        assertEquals(returnedFiles2[0], new File(tempParentDir, "version-300"));

        returnedFiles2 = ReadOnlyUtils.findKthVersionedDir(returnedFiles, 7, 7);
        assertNull(returnedFiles2);

    }
}
