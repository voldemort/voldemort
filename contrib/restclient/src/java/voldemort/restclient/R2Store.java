/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.restclient;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import voldemort.VoldemortException;
import voldemort.coordinator.VectorClockWrapper;
import voldemort.store.AbstractStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;

/**
 * A class that implements the Store interface for interacting with the RESTful
 * Coordinator. It leverages the R2 library for doing this.
 * 
 */
public class R2Store extends AbstractStore<ByteArray, byte[], byte[]> {

    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String ETAG = "ETag";
    public static final String X_VOLD_REQUEST_TIMEOUT_MS = "X-VOLD-Request-Timeout-ms";
    public static final String X_VOLD_INCONSISTENCY_RESOLVER = "X-VOLD-Inconsistency-Resolver";
    public static final String CUSTOM_RESOLVING_STRATEGY = "custom";
    public static final String DEFAULT_RESOLVING_STRATEGY = "timestamp";
    private static final String LAST_MODIFIED = "Last-Modified";
    private final Logger logger = Logger.getLogger(R2Store.class);

    HttpURLConnection conn = null;
    private HttpClientFactory _clientFactory;
    private Client client = null;
    private String baseURL;

    public R2Store(String baseURL, String storeName) {
        super(storeName);
        try {
            _clientFactory = new HttpClientFactory();
            final TransportClient transportClient = _clientFactory.getClient(new HashMap<String, String>());
            client = new TransportClientAdapter(transportClient);
            this.baseURL = baseURL;
        } catch(Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws VoldemortException {
        final FutureCallback<None> callback = new FutureCallback<None>();
        client.shutdown(callback);
        try {
            callback.get();
        } catch(InterruptedException e) {
            e.printStackTrace();
        } catch(ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean delete(ByteArray arg0, Version arg1) throws VoldemortException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {

        List<Versioned<byte[]>> resultList = new ArrayList<Versioned<byte[]>>();

        try {
            String base64Key = new String(Base64.encodeBase64(key.get()));
            RestRequestBuilder rb = new RestRequestBuilder(new URI(this.baseURL + "/test/"
                                                                   + base64Key));

            // TODO: Form a proper request based on client config
            rb.setMethod(GET);
            rb.setHeader("Accept", "application/json");
            rb.setHeader(X_VOLD_REQUEST_TIMEOUT_MS, "1000");
            rb.setHeader(X_VOLD_INCONSISTENCY_RESOLVER, "custom");

            RestRequest request = rb.build();
            Future<RestResponse> f = client.restRequest(request);

            // This will block
            RestResponse response = f.get();

            // Parse the response
            final ByteString entity = response.getEntity();
            String eTag = response.getHeader(ETAG);
            String lastModified = response.getHeader(LAST_MODIFIED);
            if(entity != null) {
                resultList = readResults(entity, eTag, lastModified);
            } else {
                logger.error("Did not get any response!");
            }

        } catch(VoldemortException ve) {
            ve.printStackTrace();
            throw ve;
        } catch(Exception e) {
            e.printStackTrace();
        }

        return resultList;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transform)
            throws VoldemortException {
        try {

            // Write the value in the payload
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(outputBytes);
            byte[] payload = value.getValue();
            outputStream.write(payload);

            // Create the REST request with this byte array
            String base64Key = new String(Base64.encodeBase64(key.get()));
            RestRequestBuilder rb = new RestRequestBuilder(new URI(this.baseURL + "/test/"
                                                                   + base64Key));

            // Create a HTTP POST request
            // TODO: Create a proper request based on client config
            rb.setMethod(POST);
            rb.setEntity(outputBytes.toByteArray());
            rb.setHeader("Content-Type", "application/json");
            rb.setHeader("Content-Length", "" + payload.length);
            rb.setHeader(X_VOLD_REQUEST_TIMEOUT_MS, "1000");
            rb.setHeader(X_VOLD_INCONSISTENCY_RESOLVER, "custom");

            RestRequest request = rb.build();
            Future<RestResponse> f = client.restRequest(request);

            // This will block
            RestResponse response = f.get();
            final ByteString entity = response.getEntity();
            if(entity == null) {
                logger.error("Empty response !");
            }
        } catch(VoldemortException ve) {
            ve.printStackTrace();
            throw ve;
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private List<Versioned<byte[]>> readResults(ByteString entity, String eTag, String lastModified)
            throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        logger.debug("Received etag : " + eTag);
        logger.debug("Received last modified date : " + lastModified);
        VectorClockWrapper vcWrapper = mapper.readValue(eTag, VectorClockWrapper.class);
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>(2);

        byte[] bytes = new byte[entity.length()];
        entity.copyBytes(bytes, 0);
        VectorClock clock = new VectorClock(vcWrapper.getVersions(), vcWrapper.getTimestamp());
        results.add(new Versioned<byte[]>(bytes, clock));
        return results;
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> arg0,
                                                          Map<ByteArray, byte[]> arg1)
            throws VoldemortException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Version> getVersions(ByteArray arg0) {
        // TODO Auto-generated method stub
        return null;
    }
}
