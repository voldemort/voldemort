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

package voldemort.store.configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A FileSystem based Storage Engine to persist configuration metadata.<br>
 * Creates/updates a File of filename 'key' for each key and write value as UTF
 * strings in it, saves version of latest entry in '.version' directory.<br>
 * Keeps a backup copy of key and old version in '.bak' directory<br>
 * This store is limited as for persisting metadata, hence some simplifications
 * are made.
 * <ul>
 * <li>Delete Operation is not permitted.</li>
 * <li>Iteration over entries is not permitted.</li>
 * <li>Store keeps a backup file and can be rolled back by copying the file to
 * master location.</li>
 * 
 * @author bbansal
 * 
 */
public class ConfigurationStorageEngine implements StorageEngine<String, String> {

    private final String name;
    private final File directory;
    private final File versionDirectory;
    private final File backupDirectory;
    private final File backupVersionDirectory;

    private static final Logger logger = Logger.getLogger(ConfigurationStorageEngine.class);

    public ConfigurationStorageEngine(String name, String directory) {
        this.name = name;
        this.directory = new File(directory);
        if(!this.directory.exists() && this.directory.canRead())
            throw new IllegalArgumentException("Directory " + this.directory.getAbsolutePath()
                                               + " does not exist or can not be read.");
        this.versionDirectory = new File(this.directory, ".version");
        this.backupDirectory = new File(this.directory, ".bak");
        this.backupVersionDirectory = new File(this.backupDirectory, ".version");

        // create version and backup directory if not exist.
        this.versionDirectory.mkdirs();
        this.backupDirectory.mkdirs();
        this.backupVersionDirectory.mkdirs();
    }

    public ClosableIterator<Pair<String, Versioned<String>>> entries() {
        throw new VoldemortException("Iteration  not supported in ConfigurationStorageEngine");
    }

    public void close() throws VoldemortException {

    }

    public synchronized boolean delete(String key, Version version) throws VoldemortException {
        throw new VoldemortException("Attempt to delete metadata for key:" + key);
    }

    public synchronized List<Versioned<String>> get(String key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return get(key, this.directory.listFiles());
    }

    private List<Versioned<String>> get(String key, File[] files) {
        try {
            List<Versioned<String>> found = new ArrayList<Versioned<String>>();
            for(File file: files) {
                if(file.getName().equals(key)) {
                    VectorClock clock = readVersion(key);
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

    public List<Version> getVersions(String key) {
        List<Versioned<String>> values = get(key);
        List<Version> versions = new ArrayList<Version>(values.size());
        for(Versioned value: values) {
            versions.add(value.getVersion());
        }
        return versions;
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
        StoreUtils.assertValidKey(key);

        if(null == value.getValue()) {
            throw new VoldemortException("metadata cannot be null !!");
        }
        // Check for obsolete version
        File[] files = this.directory.listFiles();
        for(File file: files) {
            if(file.getName().equals(key)) {
                VectorClock clock = readVersion(key);
                if(value.getVersion().compare(clock) == Occured.AFTER)
                    updateBackup(key);
                else if(value.getVersion().compare(clock) == Occured.BEFORE) {
                    throw new ObsoleteVersionException("A successor version to this exists.");
                } else if(value.getVersion().compare(clock) == Occured.CONCURRENTLY) {
                    throw new ObsoleteVersionException("Concurrent Operation not allowed on Metadata.");
                }
            }
        }

        VectorClock clock = (VectorClock) value.getVersion();
        File keyFile = new File(this.directory, key);
        if(!keyFile.exists() || keyFile.delete()) {
            try {
                FileUtils.writeStringToFile(keyFile, value.getValue(), "UTF-8");
                writeVersion(key, clock);
            } catch(IOException e) {
                try {
                    rollbackFromBackup(key);
                } catch(Exception rollbackError) {
                    logger.error("Failed to rollback with exception ", rollbackError);
                }
                throw new VoldemortException(e);
            }
        }
    }

    /**
     * rollback from backup directory
     * 
     * @param key
     */
    public boolean rollbackFromBackup(String key) {
        File backupKeyFile = new File(this.backupDirectory, key);
        File backupVersionFile = new File(this.backupVersionDirectory, key);

        if(backupKeyFile.exists() && backupVersionFile.exists()) {
            File keyFile = new File(this.directory, key);
            File versionFile = new File(this.versionDirectory, key);

            if(keyFile.delete() && versionFile.delete())
                return backupKeyFile.renameTo(keyFile) && backupVersionFile.renameTo(versionFile);

        } else {
            logger.warn("Rollback attempted but no backup File found:" + backupKeyFile);
        }

        throw new VoldemortException("Failed to rollBack for key:" + key);
    }

    /**
     * Saves the key/version file in backup directory
     * 
     * @param key
     * @return
     */
    private boolean updateBackup(String key) {
        File keyFile = new File(this.directory, key);
        File versionFile = new File(this.versionDirectory, key);
        if(!versionFile.exists()) {
            writeVersion(key, new VectorClock());
            versionFile = new File(this.versionDirectory, key);
        }

        if(keyFile.exists() && versionFile.exists()) {
            File backupKeyFile = new File(this.backupDirectory, key);
            File backupVersionFile = new File(this.backupVersionDirectory, key);

            try {
                // delete old backup
                backupKeyFile.delete();
                backupVersionFile.delete();

                return keyFile.renameTo(backupKeyFile) && versionFile.renameTo(backupVersionFile);
            } catch(Exception e) {
                logger.error("Failed to backup with exception ", e);
            }
        }

        throw new VoldemortException("Failed to take backup for key:" + key);
    }

    public Object getCapability(StoreCapabilityType capability) {
        switch(capability) {
            case ROLLBACK_FROM_BACKUP:
                return this;
            default:
                throw new NoSuchCapabilityException(capability, getName());

        }
    }

    private VectorClock readVersion(String key) {
        try {
            File versionFile = new File(this.versionDirectory, key);
            if(!versionFile.exists()) {
                // bootstrap file save default clock as version.
                VectorClock clock = new VectorClock();
                writeVersion(key, clock);
                return clock;
            } else {
                // read the version file and return version.
                String hexCode = FileUtils.readFileToString(versionFile, "UTF-8");
                return new VectorClock(Hex.decodeHex(hexCode.toCharArray()));
            }
        } catch(Exception e) {
            throw new VoldemortException("Failed to read Version for Key:" + key, e);
        }
    }

    private void writeVersion(String key, VectorClock version) {
        try {
            File versionFile = new File(this.versionDirectory, key);
            if(!versionFile.exists() || versionFile.delete()) {
                // write the version file.
                String hexCode = new String(Hex.encodeHex(version.toBytes()));
                FileUtils.writeStringToFile(versionFile, hexCode, "UTF-8");
            }
        } catch(Exception e) {
            throw new VoldemortException("Failed to write Version for Key:" + key, e);
        }
    }

    public ClosableIterator<String> keys() {
        throw new VoldemortException("Keys Iteration  not supported in ConfigurationStorageEngine");
    }
}
