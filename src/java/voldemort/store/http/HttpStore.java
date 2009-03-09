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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
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

    private static final Hex codec = new Hex();
    private static final HttpResponseCodeErrorMapper httpResponseCodeErrorMapper = new HttpResponseCodeErrorMapper();
    private static final String VERSION_EXTENSION = "X-vldmt-version";

    private final String storeName;
    private final String host;
    private final int port;
    private final HttpClient httpClient;

    public HttpStore(String storeName, String host, int port, HttpClient client) {
        this.storeName = storeName;
        this.host = host;
        this.port = port;
        this.httpClient = client;
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        String url = getUrl(key);
        DeleteMethod method = null;
        try {
            method = new DeleteMethod(url);
            VectorClock clock = (VectorClock) version;
            method.setRequestHeader(VERSION_EXTENSION,
                                    new String(Base64.encodeBase64(clock.toBytes())));
            int response = httpClient.executeMethod(method);
            if(response == HttpURLConnection.HTTP_NOT_FOUND)
                return false;
            if(response != HttpURLConnection.HTTP_OK)
                httpResponseCodeErrorMapper.throwError(response, method.getStatusText());
            return true;
        } catch(HttpException e) {
            throw new VoldemortException(e);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + url + " for " + storeName,
                                                e);
        } finally {
            if(method != null)
                method.releaseConnection();
        }
    }

    private BufferedInputStream getBufferedInputStream(InputStream inputStream) {
        if(inputStream instanceof BufferedInputStream)
            return (BufferedInputStream) inputStream;
        return new BufferedInputStream(inputStream);
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        String url = getUrl(key);
        GetMethod method = null;
        try {
            method = new GetMethod(url);
            int response = httpClient.executeMethod(method);
            if(response != HttpURLConnection.HTTP_OK)
                httpResponseCodeErrorMapper.throwError(response, method.getStatusText());
            DataInputStream input = createDataInputStream(method);
            List<Versioned<byte[]>> items = new ArrayList<Versioned<byte[]>>();
            try {
                while(true) {
                    int size = input.readInt();
                    byte[] bytes = new byte[size];
                    ByteUtils.read(input, bytes);
                    VectorClock clock = new VectorClock(bytes);
                    byte[] data = ByteUtils.copy(bytes, clock.sizeInBytes(), bytes.length);
                    items.add(new Versioned<byte[]>(data, clock));
                }
            } catch(EOFException e) {
                return items;
            } finally {
                StoreUtils.close(input);
            }
        } catch(HttpException e) {
            throw new VoldemortException(e);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + url + " for " + storeName,
                                                e);
        } finally {
            if(method != null)
                method.releaseConnection();
        }
    }

    private DataInputStream createDataInputStream(GetMethod method) throws IOException {
        return new DataInputStream(getBufferedInputStream(method.getResponseBodyAsStream()));
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        // TODO Consider retrieving the keys concurrently.
        return StoreUtils.getAll(this, keys);
    }

    public void put(ByteArray key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        String url = getUrl(key);
        PutMethod method = null;
        try {
            method = new PutMethod(url);
            VectorClock clock = (VectorClock) versioned.getVersion();
            method.setRequestHeader(VERSION_EXTENSION,
                                    new String(Base64.encodeBase64(clock.toBytes()), "UTF-8"));
            method.setRequestEntity(new ByteArrayRequestEntity(versioned.getValue()));
            int response = httpClient.executeMethod(method);
            if(response != HttpURLConnection.HTTP_OK)
                httpResponseCodeErrorMapper.throwError(response, method.getStatusText());
        } catch(HttpException e) {
            throw new VoldemortException(e);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + url + " for " + storeName,
                                                e);
        } finally {
            if(method != null)
                method.releaseConnection();
        }
    }

    public void close() {}

    public String getName() {
        return storeName;
    }

    private String getUrl(ByteArray key) throws VoldemortException {
        return "http://" + host + ":" + port + "/" + getName() + "/"
               + ByteUtils.getString(codec.encode(key.get()), "UTF-8");
    }

}
