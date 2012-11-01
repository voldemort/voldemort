package voldemort.coordinator;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
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

public class R2StoreWrapper implements Store<ByteArray, byte[], byte[]> {

    private URL url = null;
    HttpURLConnection conn = null;
    private HttpClientFactory _clientFactory;
    private Client client = null;
    private String baseURL;

    public R2StoreWrapper(String baseURL) {
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
            // Create the byte[] array
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(outputBytes);
            writeGetRequest(outputStream, key);

            // Create the REST request with this byte array
            RestRequestBuilder rb = new RestRequestBuilder(new URI(this.baseURL
                                                                   + "/get?store_name=test"));
            rb.setMethod("POST");
            rb.setEntity(outputBytes.toByteArray());
            rb.setHeader("CONTENT_TYPE", "application/pdf");

            RestRequest request = rb.build();
            Future<RestResponse> f = client.restRequest(request);

            // This will block
            RestResponse response = f.get();
            final ByteString entity = response.getEntity();
            if(entity != null) {
                // System.out.println(entity.asString("UTF-8"));
                DataInputStream inputStream = new DataInputStream(entity.asInputStream());
                resultList = readResults(inputStream);
            } else {
                System.out.println("NOTHING!");
            }

        } catch(VoldemortException ve) {
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
            // Create the byte[] array
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(outputBytes);
            writePutRequest(outputStream, key, value.getValue());

            // Create the REST request with this byte array
            RestRequestBuilder rb = new RestRequestBuilder(new URI(this.baseURL
                                                                   + "/put?store_name=test"));
            rb.setMethod("POST");
            rb.setEntity(outputBytes.toByteArray());
            rb.setHeader("CONTENT_TYPE", "application/pdf");

            RestRequest request = rb.build();
            Future<RestResponse> f = client.restRequest(request);

            // This will block
            RestResponse response = f.get();
            final ByteString entity = response.getEntity();
            if(entity != null) {
                // System.out.println(entity.asString("UTF-8"));
            } else {
                System.out.println("NOTHING!");
            }
        } catch(VoldemortException ve) {
            throw ve;
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void writeGetRequest(DataOutputStream outputStream, ByteArray key) throws IOException {
        StoreUtils.assertValidKey(key);
        outputStream.writeInt(key.length());
        outputStream.write(key.get());
    }

    private List<Versioned<byte[]>> readResults(DataInputStream inputStream) throws IOException {
        int resultSize = inputStream.readInt();
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>(resultSize);
        for(int i = 0; i < resultSize; i++) {
            int valueSize = inputStream.readInt();
            byte[] bytes = new byte[valueSize];
            ByteUtils.read(inputStream, bytes);
            VectorClock clock = new VectorClock(bytes);
            results.add(new Versioned<byte[]>(ByteUtils.copy(bytes,
                                                             clock.sizeInBytes(),
                                                             bytes.length), clock));
        }
        return results;
    }

    private void writePutRequest(DataOutputStream outputStream, ByteArray key, byte[] value)
            throws IOException {
        writeGetRequest(outputStream, key);
        outputStream.writeInt(value.length);
        outputStream.write(value);
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> arg0,
                                                          Map<ByteArray, byte[]> arg1)
            throws VoldemortException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getCapability(StoreCapabilityType arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Version> getVersions(ByteArray arg0) {
        // TODO Auto-generated method stub
        return null;
    }

}
