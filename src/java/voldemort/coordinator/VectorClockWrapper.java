/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.coordinator;

import java.util.ArrayList;
import java.util.List;

import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;

/**
 * A wrapper for Vector clock used for serialization purposes. This Wrapper is
 * then converted to a JSON string which in turn gets embedded in a HTTP header
 * field.
 * 
 */
public class VectorClockWrapper {

    private List<ClockEntry> versions;
    private long timestamp;

    public VectorClockWrapper() {
        this.versions = new ArrayList<ClockEntry>();
    }

    public VectorClockWrapper(VectorClock vc) {
        this.versions = vc.getEntries();
        this.setTimestamp(vc.getTimestamp());
    }

    public List<ClockEntry> getVersions() {
        return versions;
    }

    public void setVersions(List<ClockEntry> vectorClock) {
        versions = vectorClock;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
