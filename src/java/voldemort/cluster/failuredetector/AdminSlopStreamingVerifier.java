/*
 * Copyright 2014 LinkedIn, Inc.
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

package voldemort.cluster.failuredetector;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.SlopStreamingDisabledException;

/**
 * AdminSlopStreamingVerifier is used to verify admin connectivity and slop
 * streaming switch.
 */

public class AdminSlopStreamingVerifier extends AdminConnectionVerifier {

    public AdminSlopStreamingVerifier(Cluster cluster) {
        super(cluster);
    }

    @Override
    public void verifyStore(Node node) throws SlopStreamingDisabledException {
        Boolean enabled = Boolean.parseBoolean(getAdminClient().metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                  MetadataStore.SLOP_STREAMING_ENABLED_KEY)
                                                                               .getValue());
        if(!enabled) {
            throw new SlopStreamingDisabledException("Slop streaming is disabled on node "
                                                     + node.getId());
        }
    }

    @Override
    public void flushCachedStores() {}

}
