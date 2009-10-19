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

package voldemort.serialization;

public class VoldemortOpCode {

    public static final byte GET_OP_CODE = 1;
    public static final byte PUT_OP_CODE = 2;
    public static final byte DELETE_OP_CODE = 3;
    public static final byte GET_ALL_OP_CODE = 4;
    public static final byte GET_PARTITION_AS_STREAM_OP_CODE = 4;
    public static final byte PUT_ENTRIES_AS_STREAM_OP_CODE = 5;
    public static final byte UPDATE_METADATA_OP_CODE = 6;
    public static final byte SERVER_STATE_CHANGE_OP_CODE = 8;
    public static final byte REDIRECT_GET_OP_CODE = 9;
    public static final byte GET_VERSION_OP_CODE = 10;
}
