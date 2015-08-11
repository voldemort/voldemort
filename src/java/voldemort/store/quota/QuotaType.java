/*
 * Copyright 2008-2014 LinkedIn, Inc
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

package voldemort.store.quota;

/**
 * All possible type of quotas that can be implemented in Voldemort.
 *
 * For now, it includes the following:
 *
 * - GET_THROUGHPUT which includes all read ops (get and get all)
 * - PUT_THROUGHPUT which includes all write ops (put and delete)
 * - STORAGE_SPACE which is used exclusively for Read-Only stores
 * 
 * In the future, it could be extended for memory used, sockets etc.
 * 
 */
public enum QuotaType {
    GET_THROUGHPUT, // Includes all read ops (get and get all)
    PUT_THROUGHPUT, // Includes all write ops (put and delete)
    STORAGE_SPACE // Currently used exclusively for Read-Only stores
}
