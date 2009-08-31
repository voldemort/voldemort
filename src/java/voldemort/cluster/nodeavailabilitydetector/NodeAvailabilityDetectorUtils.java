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

import java.util.concurrent.TimeUnit;

import voldemort.client.ClientConfig;
import voldemort.server.VoldemortConfig;
import voldemort.utils.ReflectUtils;

public class NodeAvailabilityDetectorUtils {

    public static NodeAvailabilityDetector create(VoldemortConfig config) {
        return create(config.getNodeAvailabilityDetector(), config.getClientNodeBannageMs());
    }

    public static NodeAvailabilityDetector create(ClientConfig config) {
        return create(config.getNodeAvailabilityDetector(),
                      config.getNodeBannagePeriod(TimeUnit.MILLISECONDS));
    }

    private static NodeAvailabilityDetector create(String nodeAvailabilityDetectorClassName,
                                                   long nodeBannageMillis) {
        Class<?> clazz = ReflectUtils.loadClass(nodeAvailabilityDetectorClassName);
        NodeAvailabilityDetector nad = (NodeAvailabilityDetector) ReflectUtils.callConstructor(clazz,
                                                                                               new Object[] {});
        nad.setNodeBannagePeriod(nodeBannageMillis);
        return nad;
    }

}
