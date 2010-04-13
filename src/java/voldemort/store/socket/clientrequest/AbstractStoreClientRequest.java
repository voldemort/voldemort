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

package voldemort.store.socket.clientrequest;

import voldemort.client.protocol.RequestFormat;
import voldemort.server.RequestRoutingType;

public abstract class AbstractStoreClientRequest<T> extends AbstractClientRequest<T> {

    protected final String storeName;

    protected final RequestFormat requestFormat;

    protected final RequestRoutingType requestRoutingType;

    public AbstractStoreClientRequest(String storeName,
                                      RequestFormat requestFormat,
                                      RequestRoutingType requestRoutingType) {
        this.storeName = storeName;
        this.requestFormat = requestFormat;
        this.requestRoutingType = requestRoutingType;
    }

}
