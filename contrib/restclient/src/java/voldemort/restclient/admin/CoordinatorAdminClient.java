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
import voldemort.rest.coordinator.ClientConfigUtil;
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

public class CoordinatorAdminClient {

    private static final String URL_SEPARATOR = "/";
    private static final String STORE_CLIENT_CONFIG_OPS = "store-client-config-ops";

    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String DELETE = "DELETE";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CUSTOM_RESOLVING_STRATEGY = "custom";
    public static final String DEFAULT_RESOLVING_STRATEGY = "timestamp";
    public static final String SCHEMATA_STORE_NAME = "schemata";
    private static final int INVALID_ZONE_ID = -1;
    private final Logger logger = Logger.getLogger(R2Store.class);

    private Client client = null;
    private RESTClientConfig config;
    private String routingTypeCode = null;
    private int zoneId;

    public CoordinatorAdminClient() {
        this(new RESTClientConfig());
    }

    public CoordinatorAdminClient(RESTClientConfig config) {
        this.config = config;
    }

    public String getStoreClientConfigString(List<String> storeNames, String coordinatorUrl) {

        String result = null;

        try {
            // Create the REST request
            RestRequestBuilder requestBuilder = new RestRequestBuilder(new URI(coordinatorUrl
                                                                               + URL_SEPARATOR
                                                                               + STORE_CLIENT_CONFIG_OPS
                                                                               + URL_SEPARATOR
                                                                               + Joiner.on(",")
                                                                                       .join(storeNames)));

            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.GET_OP_CODE));

            requestBuilder.setMethod(GET);
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                                     String.valueOf(System.currentTimeMillis()));
            if(this.routingTypeCode != null) {
                requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE,
                                         this.routingTypeCode);
            }
            if(this.zoneId != INVALID_ZONE_ID) {
                requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ZONE_ID,
                                         String.valueOf(this.zoneId));
            }
            RestRequest request = requestBuilder.build();
            Future<RestResponse> future = client.restRequest(request);
            // This will block
            RestResponse response = future.get();
            ByteString entity = response.getEntity();
            result = entity.asString("UTF-8");
        } catch(ExecutionException e) {
            if(e.getCause() instanceof RestException) {
                RestException exception = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST EXCEPTION STATUS : " + exception.getResponse().getStatus());
                }

            } else {
                throw new VoldemortException("Unknown HTTP request execution exception: "
                                             + e.getMessage(), e);
            }
        } catch(InterruptedException e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage(), e);
            }
            throw new VoldemortException("Operation interrupted exception: " + e.getMessage(), e);
        } catch(URISyntaxException e) {
            throw new VoldemortException("Illegal HTTP URL" + e.getMessage(), e);
        }
        return result;
    }

    public boolean putStoreClientConfigString(String storeClientConfigAvro, String coordinatorUrl) {

        try {
            // Create the REST request
            RestRequestBuilder requestBuilder = new RestRequestBuilder(new URI(coordinatorUrl
                                                                               + URL_SEPARATOR
                                                                               + STORE_CLIENT_CONFIG_OPS
                                                                               + URL_SEPARATOR));

            byte[] payload = storeClientConfigAvro.getBytes("UTF-8");

            // Create a HTTP POST request
            requestBuilder.setMethod(POST);
            requestBuilder.setEntity(payload);
            requestBuilder.setHeader(CONTENT_TYPE, "binary");
            requestBuilder.setHeader(CONTENT_LENGTH, "" + payload.length);
            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.PUT_OP_CODE));
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                                     String.valueOf(System.currentTimeMillis()));
            if(this.routingTypeCode != null) {
                requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE,
                                         this.routingTypeCode);
            }
            if(this.zoneId != INVALID_ZONE_ID) {
                requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ZONE_ID,
                                         String.valueOf(this.zoneId));
            }

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

        } catch(ExecutionException e) {
            if(e.getCause() instanceof RestException) {
                RestException exception = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST EXCEPTION STATUS : " + exception.getResponse().getStatus());
                }

            } else {
                throw new VoldemortException("Unknown HTTP request execution exception: "
                                             + e.getMessage(), e);
            }
        } catch(InterruptedException e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage(), e);
            }
            throw new VoldemortException("Operation interrupted exception: " + e.getMessage(), e);
        } catch(URISyntaxException e) {
            throw new VoldemortException("Illegal HTTP URL" + e.getMessage(), e);
        } catch(UnsupportedEncodingException e) {
            throw new VoldemortException("Illegal Encoding Type " + e.getMessage());
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
            mapStoreToConfig.put(storeName,
                                 ClientConfigUtil.writeSingleClientConfigAvro(props));
        }
        return mapStoreToConfig;
    }

    public boolean putStoreClientConfigMap(Map<String, String> storeClientConfigMap,
                                           String coordinatorUrl) {
        Map<String, Properties> mapStoreToProps = Maps.newHashMap();
        for(String storeName: storeClientConfigMap.keySet()) {
            String configAvro = storeClientConfigMap.get(storeName);
            mapStoreToProps.put(storeName,
                                ClientConfigUtil.readSingleClientConfigAvro(configAvro));
        }
        return putStoreClientConfigString(ClientConfigUtil.writeMultipleClientConfigAvro(mapStoreToProps),
                                          coordinatorUrl);
    }

    public boolean deleteStoreClientConfig(List<String> storeNames, String coordinatorUrl) {

        try {
            // Create the REST request
            RestRequestBuilder requestBuilder = new RestRequestBuilder(new URI(coordinatorUrl
                                                                               + URL_SEPARATOR
                                                                               + STORE_CLIENT_CONFIG_OPS
                                                                               + URL_SEPARATOR
                                                                               + Joiner.on(",")
                                                                                       .join(storeNames)));
            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.GET_OP_CODE));
            // Create a HTTP POST request
            requestBuilder.setMethod(DELETE);
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                                     String.valueOf(System.currentTimeMillis()));
            if(this.routingTypeCode != null) {
                requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE,
                                         this.routingTypeCode);
            }
            if(this.zoneId != INVALID_ZONE_ID) {
                requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ZONE_ID,
                                         String.valueOf(this.zoneId));
            }
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

        } catch(ExecutionException e) {
            if(e.getCause() instanceof RestException) {
                RestException exception = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST EXCEPTION STATUS : " + exception.getResponse().getStatus());
                }

            } else {
                throw new VoldemortException("Unknown HTTP request execution exception: "
                                             + e.getMessage(), e);
            }
        } catch(InterruptedException e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage(), e);
            }
            throw new VoldemortException("Operation interrupted exception: " + e.getMessage(), e);
        } catch(URISyntaxException e) {
            throw new VoldemortException("Illegal HTTP URL" + e.getMessage(), e);
        }
        return false;
    }
}
