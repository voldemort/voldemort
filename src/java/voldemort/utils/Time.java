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

package voldemort.utils;

import java.util.Date;

/**
 * The interface to time itself. Oh wow, your head totally just exploded.
 * 
 * @author jay
 * 
 */
public interface Time {

    public final static int US_PER_MS = 1000;
    public final static int NS_PER_US = 1000;
    public final static int NS_PER_MS = US_PER_MS * NS_PER_US;
    public final static int MS_PER_SECOND = 1000;
    public final static int US_PER_SECOND = US_PER_MS * MS_PER_SECOND;
    public final static int NS_PER_SECOND = NS_PER_US * US_PER_SECOND;
    public final static int SECONDS_PER_HOUR = 60 * 60;
    public final static int SECONDS_PER_DAY = 24 * SECONDS_PER_HOUR;
    public final static int MS_PER_DAY = SECONDS_PER_DAY * MS_PER_SECOND;

    public long getMilliseconds();

    public long getNanoseconds();

    public int getSeconds();

    public Date getCurrentDate();

}
