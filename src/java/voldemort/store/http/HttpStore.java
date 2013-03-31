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

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.server.RequestRoutingType;
import voldemort.store.AbstractStore;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.VoldemortIOUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A remote store client that transmits operations via HTTP and interacts with
 * the VoldemortHttpServer.
 * 
 */
public class HttpStore extends AbstractStore<ByteArray, byte[], byte[]> {

    private final HttpClient httpClient;
    private final RequestFormat requestFormat;
    private final RequestRoutingType reroute;
    private final String storeUrl;

    public HttpStore(String storeName,
                     String host,
                     int port,
                     HttpClient client,
                     RequestFormat format,
                     boolean reroute) {
        super(storeName);
        this.httpClient = client;
        this.requestFormat = format;
        this.reroute = RequestRoutingType.getRequestRoutingType(reroute, false);
        this.storeUrl = "http://" + host + ":" + port + "/stores";
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DataInputStream input = null;
        try {
            HttpPost method = new HttpPost(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writeDeleteRequest(new DataOutputStream(outputBytes),
                                             getName(),
                                             key,
                                             (VectorClock) version,
                                             reroute);
            input = executeRequest(method, outputBytes);
            return requestFormat.readDeleteResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + getName(), e);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DataInputStream input = null;
        try {
            HttpPost method = new HttpPost(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writeGetRequest(new DataOutputStream(outputBytes),
                                          getName(),
                                          key,
                                          transforms,
                                          reroute);
            input = executeRequest(method, outputBytes);
            return requestFormat.readGetResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + getName(), e);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        DataInputStream input = null;
        try {
            HttpPost method = new HttpPost(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writeGetAllRequest(new DataOutputStream(outputBytes),
                                             getName(),
                                             keys,
                                             transforms,
                                             reroute);
            input = executeRequest(method, outputBytes);
            return requestFormat.readGetAllResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + getName(), e);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> versioned, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DataInputStream input = null;
        try {
            HttpPost method = new HttpPost(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writePutRequest(new DataOutputStream(outputBytes),
                                          getName(),
                                          key,
                                          versioned.getValue(),
                                          transforms,
                                          (VectorClock) versioned.getVersion(),
                                          reroute);
            input = executeRequest(method, outputBytes);
            requestFormat.readPutResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + getName(), e);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }

    private DataInputStream executeRequest(HttpPost method, ByteArrayOutputStream output) {
        HttpResponse response = null;
        try {
            method.setEntity(new ByteArrayEntity(output.toByteArray()));
            response = httpClient.execute(method);
            int statusCode = response.getStatusLine().getStatusCode();
            if(statusCode != HttpURLConnection.HTTP_OK) {
                String message = response.getStatusLine().getReasonPhrase();
                VoldemortIOUtils.closeQuietly(response);
                throw new UnreachableStoreException("HTTP request to store " + getName()
                                                    + " returned status code " + statusCode + " "
                                                    + message);
            }
            return new DataInputStream(response.getEntity().getContent());
        } catch(IOException e) {
            VoldemortIOUtils.closeQuietly(response);
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + getName(), e);
        }
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        DataInputStream input = null;
        try {
            HttpPost method = new HttpPost(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            requestFormat.writeGetVersionRequest(new DataOutputStream(outputBytes),
                                                 getName(),
                                                 key,
                                                 reroute);
            input = executeRequest(method, outputBytes);
            return requestFormat.readGetVersionResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + getName(), e);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }
}
