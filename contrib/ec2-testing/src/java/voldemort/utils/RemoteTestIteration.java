/*
 * Copyright 2009 LinkedIn, Inc.
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

package voldemort.utils;

public class RemoteTestIteration {

    private double reads;

    private double writes;

    private double deletes;

    public double getReads() {
        return reads;
    }

    public void setReads(double reads) {
        this.reads = reads;
    }

    public double getWrites() {
        return writes;
    }

    public void setWrites(double writes) {
        this.writes = writes;
    }

    public double getDeletes() {
        return deletes;
    }

    public void setDeletes(double deletes) {
        this.deletes = deletes;
    }

}
