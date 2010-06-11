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

package voldemort.hadoop;

import org.apache.hadoop.conf.Configuration;

/**
 * Common class shared by InputFormat
 * 
 */
public class VoldemortHadoopConfig {

    private static final String URL = "voldemort.url";
    private static final String STORE_NAME = "voldemort.store.name";

    public static void setVoldemortURL(Configuration conf, String url) {
        conf.set(URL, url);
    }

    public static void setVoldemortStoreName(Configuration conf, String storeName) {
        conf.set(STORE_NAME, storeName);
    }

    public static String getVoldemortURL(Configuration conf) {
        return conf.get(URL);
    }

    public static String getVoldemortStoreName(Configuration conf) {
        return conf.get(STORE_NAME);
    }

}
