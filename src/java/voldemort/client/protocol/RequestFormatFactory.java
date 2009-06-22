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

import java.util.EnumMap;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoBuffClientRequestFormat;
import voldemort.client.protocol.vold.VoldemortNativeClientRequestFormat;

/**
 * A factory for producing the appropriate client request format given a
 * {@link voldemort.client.protocol.RequestFormatType}
 * 
 * @author jay
 * 
 */
public class RequestFormatFactory {

    private EnumMap<RequestFormatType, RequestFormat> typeToInstance;

    public RequestFormatFactory() {
        this.typeToInstance = new EnumMap<RequestFormatType, RequestFormat>(RequestFormatType.class);
        this.typeToInstance.put(RequestFormatType.VOLDEMORT_V1,
                                new VoldemortNativeClientRequestFormat());
        this.typeToInstance.put(RequestFormatType.PROTOCOL_BUFFERS,
                                new ProtoBuffClientRequestFormat());
    }

    public RequestFormat getRequestFormat(RequestFormatType type) {
        RequestFormat format = this.typeToInstance.get(type);
        if(type == null)
            throw new VoldemortException("Unknown wire format " + type);
        return format;
    }

}
