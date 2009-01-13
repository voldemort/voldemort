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

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineType;

public class RandomAccessFileStorageConfiguration implements StorageConfiguration {

    private final int numFileHandles;
    private final int numBackups;
    private final long fileAccessWaitTimeoutMs;
    private final File storageDir;

    public RandomAccessFileStorageConfiguration(VoldemortConfig config) {
        this.numFileHandles = config.getReadOnlyStorageFileHandles();
        this.storageDir = new File(config.getReadOnlyDataStorageDirectory());
        this.fileAccessWaitTimeoutMs = config.getReadOnlyFileWaitTimeoutMs();
        this.numBackups = config.getReadOnlyBackups();
    }

    public void close() {}

    public StorageEngine<byte[], byte[]> getStore(String name) {
        return new RandomAccessFileStore(name,
                                         storageDir,
                                         numBackups,
                                         numFileHandles,
                                         fileAccessWaitTimeoutMs);
    }

    public StorageEngineType getType() {
        return StorageEngineType.READONLY;
    }

}
