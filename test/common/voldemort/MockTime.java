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

package voldemort;

import java.util.Date;

import voldemort.utils.Time;

public class MockTime implements Time {

    private long currentTime;

    public MockTime() {
        this.currentTime = System.currentTimeMillis();
    }

    public MockTime(long time) {
        this.currentTime = time;
    }

    public Date getCurrentDate() {
        return new Date(currentTime);
    }

    public long getMilliseconds() {
        return this.currentTime;
    }

    public long getNanoseconds() {
        return this.currentTime;
    }

    public int getSeconds() {
        return (int) (getMilliseconds() / MS_PER_SECOND);
    }

    public void setTime(long time) {
        this.currentTime = time;
    }

    public void setCurrentDate(Date date) {
        this.currentTime = date.getTime();
    }

    public void addMilliseconds(long ms) {
        this.currentTime += ms;
    }

}
