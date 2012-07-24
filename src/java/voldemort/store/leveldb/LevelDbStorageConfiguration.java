/*
 * Copyright 2008-2012 LinkedIn, Inc
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

package voldemort.store.leveldb;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

public class LevelDbStorageConfiguration implements StorageConfiguration {

    private static final Logger logger = Logger.getLogger(LevelDbStorageEngine.class);

    private final Options options;
    private final String baseDirectory;
    private final int numLocks;

    public LevelDbStorageConfiguration(VoldemortConfig config) {
        this.baseDirectory = config.getLevelDbDataDirectory();
        this.numLocks = config.getLevelDbNumLockStripes();
        this.options = new Options();
        this.options.createIfMissing(true);
        this.options.errorIfExists(false);
        this.options.maxOpenFiles(config.getLevelDbMaxOpenFiles());
        this.options.blockSize(config.getLevelDbBlockSize());
        this.options.writeBufferSize(config.getLevelDbWriteBufferSize());
        this.options.cacheSize(config.getLevelDbCacheSize());
        this.options.paranoidChecks(config.isLevelDbParanoidChecksEnabled());
        this.options.verifyChecksums(config.isLevelDbVerifyChecksumsEnabled());
        if(config.getLevelDbCompressionType() != null) {
            if("snappy".equals(config.getLevelDbCompressionType().toLowerCase()))
                this.options.compressionType(CompressionType.SNAPPY);
            else if("none".equals(config.getLevelDbCompressionType().toLowerCase()))
                this.options.compressionType(CompressionType.NONE);
            else
                throw new VoldemortException("Unknown leveldb compression type specified in config: "
                                             + config.getLevelDbCompressionType());
        }
        this.options.logger(new org.iq80.leveldb.Logger() {

            public void log(String s) {
                logger.info(s);
            }
        });
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStore(String name) {
        File file = new File(baseDirectory, name);
        file.mkdirs();
        DB db = null;
        try {
            db = factory.open(file, options);
        } catch(IOException e) {
            throw new VoldemortException("Error creating leveldb instance for store " + name, e);
        }

        return new LevelDbStorageEngine(name, db, options, file, numLocks);
    }

    public String getType() {
        return "leveldb";
    }

    public void close() {}

}
