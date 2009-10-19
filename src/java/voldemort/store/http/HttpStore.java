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

package voldemort.store.http;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A remote store client that transmits operations via HTTP and interacts with
 * the VoldemortHttpServer.
 * 
 * @author jay
 */
public class HttpStore implements Store<ByteArray, byte[]> {

    private final String storeName;
    private final HttpClient httpClient;
    private final RequestFormat requestFormat;
    private final boolean reroute;
    private final String storeUrl;

    public HttpStore(String storeName,
                     String host,
                     int port,
                     HttpClient client,
                     RequestFormat format,
                     boolean reroute) {
        this.storeName = storeName;
        this.httpClient = client;
        this.requestFormat = format;
        this.reroute = reroute;
        this.storeUrl = "http://" + host + ":" + port + "/stores";
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        PostMethod method = null;
        try {
            method = new PostMethod(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writeDeleteRequest(new DataOutputStream(outputBytes),
                                             storeName,
                                             key,
                                             (VectorClock) version,
                                             reroute);
            DataInputStream input = executeRequest(method, outputBytes);
            return requestFormat.readDeleteResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + storeName, e);
        } finally {
            if(method != null)
                method.releaseConnection();
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        PostMethod method = null;
        try {
            method = new PostMethod(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writeGetRequest(new DataOutputStream(outputBytes),
                                          storeName,
                                          key,
                                          reroute);
            DataInputStream input = executeRequest(method, outputBytes);
            return requestFormat.readGetResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + storeName, e);
        } finally {
            if(method != null)
                method.releaseConnection();
        }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        PostMethod method = null;
        try {
            method = new PostMethod(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writeGetAllRequest(new DataOutputStream(outputBytes),
                                             storeName,
                                             keys,
                                             reroute);
            DataInputStream input = executeRequest(method, outputBytes);
            return requestFormat.readGetAllResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + storeName, e);
        } finally {
            if(method != null)
                method.releaseConnection();
        }
    }

    public void put(ByteArray key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        PostMethod method = null;
        try {
            method = new PostMethod(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writePutRequest(new DataOutputStream(outputBytes),
                                          storeName,
                                          key,
                                          versioned.getValue(),
                                          (VectorClock) versioned.getVersion(),
                                          reroute);
            DataInputStream input = executeRequest(method, outputBytes);
            requestFormat.readPutResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + storeName, e);
        } finally {
            if(method != null)
                method.releaseConnection();
        }
    }

    private DataInputStream executeRequest(PostMethod method, ByteArrayOutputStream output) {
        try {
            method.setRequestEntity(new ByteArrayRequestEntity(output.toByteArray()));
            int response = httpClient.executeMethod(method);
            if(response != HttpURLConnection.HTTP_OK)
                throw new UnreachableStoreException("HTTP request to store " + storeName
                                                    + " returned status code " + response + " "
                                                    + method.getStatusText());
            return new DataInputStream(method.getResponseBodyAsStream());
        } catch(HttpException e) {
            throw new VoldemortException(e);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + storeName, e);
        }
    }

    public void close() {}

    public String getName() {
        return storeName;
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        PostMethod method = null;
        try {
            method = new PostMethod(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writeGetVersionRequest(new DataOutputStream(outputBytes),
                                                 storeName,
                                                 key,
                                                 reroute);
            DataInputStream input = executeRequest(method, outputBytes);
            return requestFormat.readGetVersionResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + storeName, e);
        } finally {
            if(method != null)
                method.releaseConnection();
        }
    }
}
