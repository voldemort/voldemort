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

package voldemort.client.protocol;

import voldemort.client.protocol.pb.ProtoBuffClientRequestFormat;
import voldemort.client.protocol.vold.VoldemortNativeClientRequestFormat;

/**
 * A factory for producing the appropriate client request format given a
 * {@link voldemort.client.protocol.RequestFormatType}
 * 
 * 
 */
public class RequestFormatFactory {

    public RequestFormatFactory() {}

    public RequestFormat getRequestFormat(RequestFormatType type) {
        switch(type) {
            case VOLDEMORT_V1:
                return new VoldemortNativeClientRequestFormat(1);
            case VOLDEMORT_V2:
                return new VoldemortNativeClientRequestFormat(2);
            case VOLDEMORT_V3:
                return new VoldemortNativeClientRequestFormat(3);
            case PROTOCOL_BUFFERS:
                return new ProtoBuffClientRequestFormat();
            default:
                throw new IllegalArgumentException("Unknown request format type: " + type);
        }
    }

}
