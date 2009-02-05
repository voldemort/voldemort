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

package test.voldemort.contrib.utils;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;

import voldemort.contrib.utils.ContribUtils;

public class TestContribUtils extends TestCase {

    public void testGetFileFromPathList() {
        Path path1 = new Path("simpleFileName");
        Path path2 = new Path("somePath/simpleFileName");
        Path path3 = new Path("somePath/someOtherPath/");
        Path path4 = new Path("file:////somepath/simpleFileName");

        assertEquals("Returned Path should match",
                     path1.toString(),
                     ContribUtils.getFileFromPathList(new Path[] { path1 }, "simpleFileName"));
        assertEquals("Returned Path should match",
                     path2.toString(),
                     ContribUtils.getFileFromPathList(new Path[] { path2 }, "simpleFileName"));
        assertEquals("Returned Path should be null",
                     null,
                     ContribUtils.getFileFromPathList(new Path[] { path3 }, "simpleFileName"));

        assertNotSame("Returned Path should Not match schema part",
                      path4.toString(),
                      ContribUtils.getFileFromPathList(new Path[] { path4 }, "simpleFileName"));

        assertEquals("Returned path should match core path w/o schema",
                     path4.toUri().getPath(),
                     ContribUtils.getFileFromPathList(new Path[] { path4 }, "simpleFileName"));

        assertEquals("Returned Path should match",
                     path1.toString(),
                     ContribUtils.getFileFromPathList(new Path[] { path1, path2 }, "simpleFileName"));

        assertEquals("Returned Path should match",
                     path1.toString(),
                     ContribUtils.getFileFromPathList(new Path[] { path3, path1, path2 },
                                                      "simpleFileName"));

        assertEquals("Returned Path should match",
                     path2.toString(),
                     ContribUtils.getFileFromPathList(new Path[] { path3, path2, path1 },
                                                      "simpleFileName"));
    }
}
