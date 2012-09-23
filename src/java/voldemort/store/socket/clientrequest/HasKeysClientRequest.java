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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import voldemort.client.protocol.RequestFormat;
import voldemort.server.RequestRoutingType;
import voldemort.utils.ByteArray;

public class HasKeysClientRequest extends AbstractStoreClientRequest<Map<ByteArray, Boolean>> {

    private final Iterable<ByteArray> keys;
    private final boolean exact;

    public HasKeysClientRequest(String storeName,
                                RequestFormat requestFormat,
                                RequestRoutingType requestRoutingType,
                                Iterable<ByteArray> keys,
                                boolean exact) {
        super(storeName, requestFormat, requestRoutingType);
        this.keys = keys;
        this.exact = exact;
    }

    public boolean isCompleteResponse(ByteBuffer buffer) {
        return requestFormat.isCompleteHasKeysResponse(buffer);
    }

    @Override
    protected void formatRequestInternal(DataOutputStream outputStream) throws IOException {
        requestFormat.writeHasKeysRequest(outputStream, storeName, keys, exact, requestRoutingType);
    }

    @Override
    protected Map<ByteArray, Boolean> parseResponseInternal(DataInputStream inputStream)
            throws IOException {
        return requestFormat.readHasKeysResponse(inputStream);
    }

}
