package voldemort.restclient.admin;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.common.VoldemortOpCode;
import voldemort.rest.RestMessageHeaders;
import voldemort.rest.coordinator.config.ClientConfigUtil;
import voldemort.restclient.R2Store;
import voldemort.restclient.RESTClientConfig;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;

public class CoordinatorAdminClient {

    private static final String URL_SEPARATOR = "/";
    private static final String STORE_CLIENT_CONFIG_OPS = "store-client-config-ops";

    private static enum requestType {
        GET,
        POST,
        DELETE
    }

    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CUSTOM_RESOLVING_STRATEGY = "custom";
    public static final String DEFAULT_RESOLVING_STRATEGY = "timestamp";
    public static final String SCHEMATA_STORE_NAME = "schemata";
    private static final int INVALID_ZONE_ID = -1;
    private final Logger logger = Logger.getLogger(R2Store.class);

    private HttpClientFactory httpClientFactory;
    private Client client;
    private RESTClientConfig config;
    private String routingTypeCode = null;
    private int zoneId;

    public CoordinatorAdminClient() {
        this(new RESTClientConfig());
    }

    public CoordinatorAdminClient(RESTClientConfig config) {
        this.config = config;
        this.httpClientFactory = new HttpClientFactory();
        Map<String, String> properties = Maps.newHashMap();
        properties.put(HttpClientFactory.HTTP_POOL_SIZE,
                       Integer.toString(this.config.getMaxR2ConnectionPoolSize()));
        this.client = new TransportClientAdapter(httpClientFactory.getClient(properties));
    }

    private RestRequestBuilder setCommonRequestHeader(RestRequestBuilder requestBuilder) {
        requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                                 String.valueOf(System.currentTimeMillis()));
        if(this.routingTypeCode != null) {
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE,
                                     this.routingTypeCode);
        }
        if(this.zoneId != INVALID_ZONE_ID) {
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ZONE_ID, String.valueOf(this.zoneId));
        }
        return requestBuilder;
    }

    private void handleRequestAndResponseException(Exception e) {
        if(e instanceof ExecutionException) {
            if(e.getCause() instanceof RestException) {
                RestException re = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST Exception Status: " + re.getResponse().getStatus());
                }
            } else {
                throw new VoldemortException("Unknown HTTP request execution exception: "
                                             + e.getMessage(), e);
            }
        } else if(e instanceof InterruptedException) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage(), e);
            }
            throw new VoldemortException("Operation interrupted exception: " + e.getMessage(), e);
        } else if(e instanceof URISyntaxException) {
            throw new VoldemortException("Illegal HTTP URL " + e.getMessage(), e);
        } else if(e instanceof UnsupportedEncodingException) {
            throw new VoldemortException("Illegal Encoding Type " + e.getMessage());
        } else {
            throw new VoldemortException("Unknown exception: " + e.getMessage(), e);
        }
    }

    public String getStoreClientConfigString(List<String> storeNames, String coordinatorUrl) {
        try {
            // Create the REST request
            StringBuilder URIStringBuilder = new StringBuilder().append(coordinatorUrl)
                                                                .append(URL_SEPARATOR)
                                                                .append(STORE_CLIENT_CONFIG_OPS)
                                                                .append(URL_SEPARATOR)
                                                                .append(Joiner.on(",")
                                                                              .join(storeNames));

            RestRequestBuilder requestBuilder = new RestRequestBuilder(new URI(URIStringBuilder.toString()));

            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.GET_OP_CODE));

            requestBuilder.setMethod(requestType.GET.toString());
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
            requestBuilder = setCommonRequestHeader(requestBuilder);
            RestRequest request = requestBuilder.build();
            Future<RestResponse> future = client.restRequest(request);
            // This will block
            RestResponse response = future.get();
            ByteString entity = response.getEntity();
            return entity.asString("UTF-8");
        } catch(Exception e) {
            if(e.getCause() instanceof RestException) {
                return ((RestException) e.getCause()).getResponse().getEntity().asString("UTF-8");
            }
            handleRequestAndResponseException(e);
        }
        return null;
    }

    public boolean putStoreClientConfigString(String storeClientConfigAvro, String coordinatorUrl) {

        try {
            // Create the REST request
            StringBuilder URIStringBuilder = new StringBuilder().append(coordinatorUrl)
                                                                .append(URL_SEPARATOR)
                                                                .append(STORE_CLIENT_CONFIG_OPS)
                                                                .append(URL_SEPARATOR);
            RestRequestBuilder requestBuilder = new RestRequestBuilder(new URI(URIStringBuilder.toString()));

            byte[] payload = storeClientConfigAvro.getBytes("UTF-8");

            // Create a HTTP POST request
            requestBuilder.setMethod(requestType.POST.toString());
            requestBuilder.setEntity(payload);
            requestBuilder.setHeader(CONTENT_TYPE, "binary");
            requestBuilder.setHeader(CONTENT_LENGTH, "" + payload.length);
            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.PUT_OP_CODE));
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
            requestBuilder = setCommonRequestHeader(requestBuilder);

            RestRequest request = requestBuilder.build();
            Future<RestResponse> future = client.restRequest(request);

            // This will block
            RestResponse response = future.get();
            final ByteString entity = response.getEntity();
            if(entity == null) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Empty response !");
                }
                return false;
            }
            System.out.println(entity.asString("UTF-8"));
            return true;
        } catch(Exception e) {
            handleRequestAndResponseException(e);
        }
        return false;
    }

    public Map<String, String> getStoreClientConfigMap(List<String> storeNames,
                                                       String coordinatorUrl) {
        String configAvro = getStoreClientConfigString(storeNames, coordinatorUrl);
        Map<String, Properties> mapStoreToProps = ClientConfigUtil.readMultipleClientConfigAvro(configAvro);
        Map<String, String> mapStoreToConfig = Maps.newHashMap();
        for(String storeName: mapStoreToProps.keySet()) {
            Properties props = mapStoreToProps.get(storeName);
            mapStoreToConfig.put(storeName, ClientConfigUtil.writeSingleClientConfigAvro(props));
        }
        return mapStoreToConfig;
    }

    public boolean putStoreClientConfigMap(Map<String, String> storeClientConfigMap,
                                           String coordinatorUrl) {
        Map<String, Properties> mapStoreToProps = Maps.newHashMap();
        for(String storeName: storeClientConfigMap.keySet()) {
            String configAvro = storeClientConfigMap.get(storeName);
            mapStoreToProps.put(storeName, ClientConfigUtil.readSingleClientConfigAvro(configAvro));
        }
        return putStoreClientConfigString(ClientConfigUtil.writeMultipleClientConfigAvro(mapStoreToProps),
                                          coordinatorUrl);
    }

    public boolean deleteStoreClientConfig(List<String> storeNames, String coordinatorUrl) {
        try {
            // Create the REST request
            StringBuilder URIStringBuilder = new StringBuilder().append(coordinatorUrl)
                                                                .append(URL_SEPARATOR)
                                                                .append(STORE_CLIENT_CONFIG_OPS)
                                                                .append(URL_SEPARATOR)
                                                                .append(Joiner.on(",")
                                                                              .join(storeNames));
            RestRequestBuilder requestBuilder = new RestRequestBuilder(new URI(URIStringBuilder.toString()));
            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.GET_OP_CODE));
            // Create a HTTP POST request
            requestBuilder.setMethod(requestType.DELETE.toString());
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
            requestBuilder = setCommonRequestHeader(requestBuilder);

            RestRequest request = requestBuilder.build();
            Future<RestResponse> future = client.restRequest(request);
            // This will block
            RestResponse response = future.get();
            final ByteString entity = response.getEntity();
            if(entity == null) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Empty response !");
                }
                return false;
            }
            System.out.println(entity.asString("UTF-8"));
            return true;
        } catch(Exception e) {
            handleRequestAndResponseException(e);
        }
        return false;
    }
}
