/*
 * Copyright 2009 Mustard Grain, Inc.
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

package voldemort.cluster.nodeavailabilitydetector;

import java.util.Map;

import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ReflectUtils;

import com.google.common.collect.Maps;

public class NodeAvailabilityDetectorUtils {

    public static NodeAvailabilityDetector create(String implementationClassName,
                                                  long nodeBannagePeriod) {
        Map<Integer, Store<ByteArray, byte[]>> stores = Maps.newHashMap();
        return create(implementationClassName, nodeBannagePeriod, stores);
    }

    public static NodeAvailabilityDetector create(String implementationClassName,
                                                  long nodeBannagePeriod,
                                                  Map<Integer, Store<ByteArray, byte[]>> stores) {
        NodeAvailabilityDetectorConfig config = new BasicNodeAvailabilityDetectorConfig(implementationClassName,
                                                                                        nodeBannagePeriod,
                                                                                        stores);
        return create(config);
    }

    public static NodeAvailabilityDetector create(VoldemortConfig voldemortConfig,
                                                  StoreRepository storeRepository) {
        NodeAvailabilityDetectorConfig config = new ServerNodeAvailabilityDetectorConfig(voldemortConfig,
                                                                                         storeRepository);
        return create(config);
    }

    public static NodeAvailabilityDetector create(NodeAvailabilityDetectorConfig nodeAvailabilityDetectorConfig) {
        Class<?> clazz = ReflectUtils.loadClass(nodeAvailabilityDetectorConfig.getImplementationClassName());
        NodeAvailabilityDetector nad = (NodeAvailabilityDetector) ReflectUtils.callConstructor(clazz,
                                                                                               new Class[] { NodeAvailabilityDetectorConfig.class },
                                                                                               new Object[] { nodeAvailabilityDetectorConfig });
        return nad;
    }

}
