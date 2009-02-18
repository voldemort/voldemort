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
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineType;
import voldemort.utils.JmxUtils;

public class RandomAccessFileStorageConfiguration implements StorageConfiguration {

    private final int numFileHandles;
    private final int numBackups;
    private final long fileAccessWaitTimeoutMs;
    private final File storageDir;
    private final Set<ObjectName> registeredBeans;
    private final long cacheSize;

    public RandomAccessFileStorageConfiguration(VoldemortConfig config) {
        this.numFileHandles = config.getReadOnlyStorageFileHandles();
        this.storageDir = new File(config.getReadOnlyDataStorageDirectory());
        this.fileAccessWaitTimeoutMs = config.getReadOnlyFileWaitTimeoutMs();
        this.numBackups = config.getReadOnlyBackups();
        this.registeredBeans = Collections.synchronizedSet(new HashSet<ObjectName>());
        this.cacheSize = config.getReadOnlyCacheSize();
    }

    public void close() {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        for(ObjectName name: registeredBeans)
            JmxUtils.unregisterMbean(server, name);
    }

    public StorageEngine<byte[], byte[]> getStore(String name) {
        RandomAccessFileStore store = new RandomAccessFileStore(name,
                                                                storageDir,
                                                                numBackups,
                                                                numFileHandles,
                                                                fileAccessWaitTimeoutMs,
                                                                cacheSize);
        ObjectName objName = JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
                                                       name);
        JmxUtils.registerMbean(ManagementFactory.getPlatformMBeanServer(),
                               JmxUtils.createModelMBean(store),
                               objName);
        registeredBeans.add(objName);

        return store;
    }

    public StorageEngineType getType() {
        return StorageEngineType.READONLY;
    }

}
