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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import voldemort.server.RequestRoutingType;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Abstracts the serialization mechanism used to write a client request. The
 * companion class on the server side is
 * {@link voldemort.server.protocol.RequestHandler}
 * 
 * 
 */
public interface RequestFormat {

    public void writeGetRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                byte[] transforms,
                                RequestRoutingType routingType) throws IOException;

    public boolean isCompleteGetResponse(ByteBuffer buffer);

    public List<Versioned<byte[]>> readGetResponse(DataInputStream stream) throws IOException;

    public void writeGetVersionRequest(DataOutputStream output,
                                       String storeName,
                                       ByteArray key,
                                       RequestRoutingType routingType) throws IOException;

    public boolean isCompleteGetVersionResponse(ByteBuffer buffer);

    public List<Version> readGetVersionResponse(DataInputStream stream) throws IOException;

    public void writeGetAllRequest(DataOutputStream output,
                                   String storeName,
                                   Iterable<ByteArray> key,
                                   Map<ByteArray, byte[]> transforms,
                                   RequestRoutingType routingType) throws IOException;

    public boolean isCompleteGetAllResponse(ByteBuffer buffer);

    public Map<ByteArray, List<Versioned<byte[]>>> readGetAllResponse(DataInputStream stream)
            throws IOException;

    public void writePutRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                byte[] value,
                                byte[] transforms,
                                VectorClock version,
                                RequestRoutingType routingType) throws IOException;

    public boolean isCompletePutResponse(ByteBuffer buffer);

    public void readPutResponse(DataInputStream stream) throws IOException;

    public void writeDeleteRequest(DataOutputStream output,
                                   String storeName,
                                   ByteArray key,
                                   VectorClock version,
                                   RequestRoutingType routingType) throws IOException;

    public boolean isCompleteDeleteResponse(ByteBuffer buffer);

    public boolean readDeleteResponse(DataInputStream input) throws IOException;
}
