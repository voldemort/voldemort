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

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;
import voldemort.utils.ReflectUtils;

public class ReadOnlyStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "read-only";

    private final int numBackups;
    private final File storageDir;
    private final Set<ObjectName> registeredBeans;
    private final SearchStrategy searcher;
    private final int nodeId;
    private RoutingStrategy routingStrategy = null;
    private final int deleteBackupMs;
    private boolean enforceMlock = false;

    public ReadOnlyStorageConfiguration(VoldemortConfig config) {
        this.storageDir = new File(config.getReadOnlyDataStorageDirectory());
        this.numBackups = config.getNumReadOnlyVersions();
        this.registeredBeans = Collections.synchronizedSet(new HashSet<ObjectName>());
        this.searcher = (SearchStrategy) ReflectUtils.callConstructor(ReflectUtils.loadClass(config.getReadOnlySearchStrategy()
                                                                                                   .trim()));
        this.nodeId = config.getNodeId();
        this.deleteBackupMs = config.getReadOnlyDeleteBackupMs();
        this.enforceMlock = config.isUseMlock();
    }

    public void close() {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        for(ObjectName name: registeredBeans)
            JmxUtils.unregisterMbean(server, name);
    }

    public void setRoutingStrategy(RoutingStrategy routingStrategy) {
        this.routingStrategy = routingStrategy;
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy) {
        this.setRoutingStrategy(strategy);
        ReadOnlyStorageEngine store = new ReadOnlyStorageEngine(storeDef.getName(),
                                                                this.searcher,
                                                                this.routingStrategy,
                                                                this.nodeId,
                                                                new File(storageDir,
                                                                         storeDef.getName()),
                                                                numBackups,
                                                                deleteBackupMs,
                                                                enforceMlock);
        ObjectName objName = JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
                                                       storeDef.getName() + nodeId);
        JmxUtils.registerMbean(ManagementFactory.getPlatformMBeanServer(),
                               JmxUtils.createModelMBean(store),
                               objName);
        registeredBeans.add(objName);

        return store;
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void update(StoreDefinition storeDef) {
        throw new VoldemortException("Storage config updates not permitted for "
                                     + this.getClass().getCanonicalName());
    }
}
