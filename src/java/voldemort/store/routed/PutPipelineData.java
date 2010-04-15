/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed;

import voldemort.cluster.Node;
import voldemort.versioning.Versioned;

public class PutPipelineData extends ListStateData {

    private Node master;

    private Versioned<byte[]> versionedCopy;

    public Node getMaster() {
        return master;
    }

    public void setMaster(Node master) {
        this.master = master;
    }

    public Versioned<byte[]> getVersionedCopy() {
        return versionedCopy;
    }

    public void setVersionedCopy(Versioned<byte[]> versionedCopy) {
        this.versionedCopy = versionedCopy;
    }

}
