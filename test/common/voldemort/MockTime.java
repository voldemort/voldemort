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
import java.util.concurrent.TimeUnit;

import voldemort.utils.Time;

public class MockTime implements Time {

    private long timeMs;

    public MockTime() {
        this.timeMs = System.currentTimeMillis();
    }

    public MockTime(long time) {
        this.timeMs = time;
    }

    public Date getCurrentDate() {
        return new Date(timeMs);
    }

    public long getMilliseconds() {
        return this.timeMs;
    }

    public long getNanoseconds() {
        return this.timeMs * Time.NS_PER_MS;
    }

    public int getSeconds() {
        return (int) (timeMs / MS_PER_SECOND);
    }

    @Override
    public long milliseconds() {
        return getMilliseconds();
    }

    @Override
    public long nanoseconds() {
        return getNanoseconds();
    }

    public void sleep(long ms) {
        addMilliseconds(ms);
    }

    public void setTime(long ms) {
        this.timeMs = ms;
    }

    public void setCurrentDate(Date date) {
        this.timeMs = date.getTime();
    }

    public void addMilliseconds(long ms) {
        this.timeMs += ms;
    }

    public void add(long duration, TimeUnit units) {
        this.timeMs += TimeUnit.MILLISECONDS.convert(duration, units);
    }

}
