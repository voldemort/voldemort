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

package voldemort.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Helper functions FTW!
 * 
 * @author jay
 * 
 */
public class Utils {

    public static final String NEWLINE = System.getProperty("line.separator");

    /**
     * Print an error and exit with error code 1
     * 
     * @param message The error to print
     */
    public static void croak(String message) {
        System.err.println(message);
        System.exit(1);
    }

    /**
     * Print an error and exit with the given error code
     * 
     * @param message The error to print
     * @param errorCode The error code to exit with
     */
    public static void croak(String message, int errorCode) {
        System.err.println(message);
        System.exit(errorCode);
    }

    /**
     * Combine the given items as a list
     * 
     * @param <T> The type of the items
     * @param args The items to combine
     * @return A list of the items
     */
    public static <T> List<T> asList(T... args) {
        List<T> items = new ArrayList<T>();
        Collections.addAll(items, args);
        return items;
    }

    /**
     * Delete the given file
     * 
     * @param file The file to delete
     */
    public static void rm(File file) {
        rm(Collections.singletonList(file));
    }

    /**
     * Delete the given file
     * 
     * @param file The file to delete
     */
    public static void rm(String file) {
        rm(Collections.singletonList(new File(file)));
    }

    /**
     * Delete all the given files
     * 
     * @param files A collection of files to delete
     */
    public static void rm(Collection<File> files) {
        for(File f: files) {
            if(f.isDirectory()) {
                rm(Arrays.asList(f.listFiles()));
                f.delete();
            } else {
                f.delete();
            }
        }
    }

    public static boolean isReadableFile(String fileName) {
        File f = new File(fileName);
        return f.exists() && f.isFile() && f.canRead();
    }

    public static boolean isReadableDir(String dirName) {
        File d = new File(dirName);
        return d.exists() && d.isDirectory() && d.canRead();
    }

    /**
     * Check if the two objects are equal to one another if t1 == t2 they are
     * equal if t1 != t2 and one is null, they are not equal to each other if
     * t1.equals(t2) they are equal to each other
     * 
     * @param o1 The first object
     * @param o2 The second object
     * @return True iff they are equal
     */
    public static boolean areNEquals(Object o1, Object o2) {
        if(o1 == o2)
            return true;
        // t1 != t2
        else if(o1 == null || o2 == null)
            // only null equals null
            return false;
        // are they of the same class?
        else if(!o1.getClass().equals(o2.getClass()))
            return false;
        // t1 != null
        else
            return o1.equals(o2);
    }

    /**
     * Throw an IllegalArgumentException if any of the given objects are null
     * 
     * @param objects The objects to test
     */
    public static void assertNotNull(Object... objects) {
        assertNotNull("Null argument not allowed", objects);
    }

    /**
     * Throw an IllegalArgumentException if any of the given objects are null
     * 
     * @param s The error message to give
     * @param objects The objects to test
     */
    public static void assertNotNull(String s, Object... objects) {
        for(Object o: objects)
            if(o == null)
                throw new IllegalArgumentException(s);
    }

}
