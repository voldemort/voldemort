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

package voldemort.store.filesystem;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class FilesystemStorageEngine implements StorageEngine<String, String> {

    private final String name;
    private final File directory;

    private static final Logger logger = Logger.getLogger(FilesystemStorageEngine.class);

    public FilesystemStorageEngine(String name, String directory) {
        this.name = name;
        this.directory = new File(directory);
        if(!this.directory.exists() && this.directory.canRead())
            throw new IllegalArgumentException("Directory " + this.directory.getAbsolutePath()
                                               + " does not exist or can not be read.");
    }

    public ClosableIterator<Pair<String, Versioned<String>>> entries() {
        return new FilesystemClosableIterator();
    }

    public void close() throws VoldemortException {

    }

    public synchronized boolean delete(String key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        File[] files = this.directory.listFiles();
        boolean deletedSomething = false;
        for(File file: files) {
            if(file.getName().startsWith(key)) {
                VectorClock clock = getVersion(file, key);
                if(null != clock && clock.compare(version) == Occured.BEFORE)
                    deletedSomething |= file.delete();
            }
        }
        return deletedSomething;
    }

    public synchronized List<Versioned<String>> get(String key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return get(key, this.directory.listFiles());
    }

    private List<Versioned<String>> get(String key, File[] files) {
        try {
            List<Versioned<String>> found = new ArrayList<Versioned<String>>();
            for(File file: files) {
                if(file.getName().startsWith(key)) {
                    VectorClock clock = getVersion(file, key);
                    if(null != clock) {
                        found.add(new Versioned<String>(FileUtils.readFileToString(file, "UTF-8"),
                                                        clock));
                    }
                }
            }
            return found;
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    public synchronized Map<String, List<Versioned<String>>> getAll(Iterable<String> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<String, List<Versioned<String>>> result = StoreUtils.newEmptyHashMap(keys);
        for(String key: keys) {
            List<Versioned<String>> values = get(key, this.directory.listFiles());
            if(!values.isEmpty())
                result.put(key, values);
        }
        return result;
    }

    public String getName() {
        return name;
    }

    public synchronized void put(String key, Versioned<String> value) throws VoldemortException {
        ArrayList<String> deleteList = new ArrayList<String>();
        StoreUtils.assertValidKey(key);
        // Check for obsolete version
        File[] files = this.directory.listFiles();
        for(File file: files) {
            if(file.getName().startsWith(key)) {
                VectorClock clock = getVersion(file, key);
                if(null != clock) {
                    if(clock.compare(value.getVersion()) == Occured.AFTER)
                        throw new ObsoleteVersionException("A successor version to this exists.");
                    else if(clock.compare(value.getVersion()) == Occured.BEFORE) {
                        // Add the file to deleteList
                        deleteList.add(file.getAbsolutePath());
                    }
                }
            }
        }

        VectorClock clock = (VectorClock) value.getVersion();
        String path = this.directory.getAbsolutePath() + File.separator + key + "-"
                      + new String(Hex.encodeHex(clock.toBytes())) + ".version";
        File newFile = new File(path);
        try {
            if(!newFile.createNewFile())
                throw new ObsoleteVersionException("File " + path + " already exists.");
            FileUtils.writeStringToFile(newFile, value.getValue(), "UTF-8");
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        // if succceded remove the old value Object.
        for(String file: deleteList) {
            try {
                FileDeleteStrategy.FORCE.delete(new File(file));
            } catch(IOException e) {
                logger.warn("Failed to Delete File:" + file);
            }
        }
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    private VectorClock getVersion(File file, String key) {
        try {
            int index = file.getName().lastIndexOf('-');
            if(index <= 0) {
                // the filename should match key exactly
                if(file.getName().equals(key)) {
                    return new VectorClock();
                }
            } else if(file.getName().endsWith(".version")) {
                return new VectorClock(Hex.decodeHex(file.getName()
                                                         .replace(".version", "")
                                                         .substring(index + 1)
                                                         .toCharArray()));
            }
        } catch(DecoderException e) {
            throw new VoldemortException(e);
        }

        // if filename is not recognized
        return null;
    }

    private class FilesystemClosableIterator implements
            ClosableIterator<Pair<String, Versioned<String>>> {

        private File[] files;
        private int index;

        public FilesystemClosableIterator() {
            this.files = directory.listFiles();
            this.index = 0;
        }

        public void close() {
            this.files = null;
        }

        public boolean hasNext() {
            return this.files != null && this.index < this.files.length;
        }

        public Pair<String, Versioned<String>> next() {
            synchronized(FilesystemStorageEngine.this) {
                while(true) {
                    if(!hasNext())
                        throw new NoSuchElementException("No more elements in iterator!");

                    try {
                        String name = files[index].getName();
                        int split = name.lastIndexOf('-');
                        String key = split < 0 ? name : name.substring(0, split);
                        VectorClock clock = getVersion(files[index], key);
                        if(null != clock) {
                            String value = FileUtils.readFileToString(files[index]);
                            this.index++;
                            return Pair.create(key, new Versioned<String>(value, clock));
                        }
                    } catch(IOException e) {
                        // probably the file has been removed or something, skip
                        // it
                    }
                }
            }
        }

        public void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }
    }

    public List<Version> getVersions(String key) {
        return StoreUtils.getVersions(get(key));
    }
}
