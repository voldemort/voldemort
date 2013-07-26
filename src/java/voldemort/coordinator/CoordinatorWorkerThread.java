package voldemort.coordinator;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;

import voldemort.common.VoldemortOpCode;
import voldemort.server.rest.GetAllResponseSender;
import voldemort.server.rest.GetMetadataResponseSender;
import voldemort.server.rest.GetResponseSender;
import voldemort.server.rest.PutResponseSender;
import voldemort.server.rest.RestServerDeleteErrorHandler;
import voldemort.server.rest.RestServerGetErrorHandler;
import voldemort.server.rest.RestServerGetVersionErrorHandler;
import voldemort.server.rest.RestServerPutErrorHandler;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class CoordinatorWorkerThread implements Runnable {

    private final static RestServerGetErrorHandler getErrorHandler = new RestServerGetErrorHandler();
    private final static RestServerGetVersionErrorHandler getVersionErrorHandler = new RestServerGetVersionErrorHandler();
    private final static RestServerPutErrorHandler putErrorHandler = new RestServerPutErrorHandler();
    private final static RestServerDeleteErrorHandler deleteErrorHandler = new RestServerDeleteErrorHandler();

    private MessageEvent messageEvent;
    CompositeVoldemortRequest<ByteArray, byte[]> requestObject;
    private DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient = null;
    private final CoordinatorMetadata coordinatorMetadata;
    private final Logger logger = Logger.getLogger(getClass());

    public CoordinatorWorkerThread(MessageEvent channelEvent,
                                   CoordinatorMetadata coordinatorMetadata) {
        this.messageEvent = channelEvent;
        this.coordinatorMetadata = coordinatorMetadata;
    }

    @Override
    // TODO: Add perf stats in the next iteration
    public void run() {
        Object message = messageEvent.getMessage();
        if(message instanceof CoordinatorStoreClientRequest) {
            CoordinatorStoreClientRequest storeClientRequestObject = (CoordinatorStoreClientRequest) message;
            this.requestObject = storeClientRequestObject.getRequestObject();
            this.storeClient = storeClientRequestObject.getStoreClient();

            // This shouldn't ideally happen.
            if(this.requestObject != null) {

                switch(requestObject.getOperationType()) {
                    case VoldemortOpCode.GET_METADATA_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("GET Metadata request received.");
                        }

                        try {

                            String queryStoreName = ByteUtils.getString(this.requestObject.getKey()
                                                                                          .get(),
                                                                        "UTF-8");
                            StoreDefinition storeDef = StoreDefinitionUtils.getStoreDefinitionWithName(this.coordinatorMetadata.getStoresDefs(),
                                                                                                       queryStoreName);
                            String serializerInfoXml = CoordinatorUtils.constructSerializerInfoXml(storeDef);
                            GetMetadataResponseSender metadataResponseSender = new GetMetadataResponseSender(messageEvent,
                                                                                                             serializerInfoXml.getBytes());

                            metadataResponseSender.sendResponse();
                            if(logger.isDebugEnabled()) {
                                logger.debug("GET Metadata successful !");
                            }
                        } catch(Exception e) {
                            getErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    case VoldemortOpCode.GET_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("GET request received.");
                        }

                        try {
                            boolean keyExists = false;
                            List<Versioned<byte[]>> versionedValues = this.storeClient.getWithCustomTimeout(this.requestObject);
                            if(versionedValues == null || versionedValues.size() == 0) {
                                if(this.requestObject.getValue() != null) {
                                    if(versionedValues == null) {
                                        versionedValues = new ArrayList<Versioned<byte[]>>();
                                    }
                                    versionedValues.add(this.requestObject.getValue());
                                    keyExists = true;

                                }
                            } else {
                                keyExists = true;
                            }

                            if(keyExists) {
                                GetResponseSender responseConstructor = new GetResponseSender(messageEvent,
                                                                                              requestObject.getKey(),
                                                                                              versionedValues,
                                                                                              this.storeClient.getStoreName());
                                responseConstructor.sendResponse();
                                if(logger.isDebugEnabled()) {
                                    logger.debug("GET successful !");
                                }

                            } else {

                                RESTErrorHandler.handleError(NOT_FOUND,
                                                             this.messageEvent,
                                                             "Requested Key does not exist");
                            }

                        } catch(Exception e) {
                            getErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    case VoldemortOpCode.GET_ALL_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("GET ALL request received.");
                        }

                        try {
                            Map<ByteArray, List<Versioned<byte[]>>> versionedResponses = this.storeClient.getAllWithCustomTimeout(this.requestObject);
                            if(versionedResponses == null
                               || versionedResponses.values().size() == 0) {
                                logger.error("Error when doing getall. Keys do not exist.");
                                RESTErrorHandler.handleError(NOT_FOUND,
                                                             this.messageEvent,
                                                             "Error when doing getall. Keys do not exist.");
                            } else {
                                GetAllResponseSender responseConstructor = new GetAllResponseSender(messageEvent,
                                                                                                    versionedResponses,
                                                                                                    this.storeClient.getStoreName());
                                responseConstructor.sendResponse();

                                if(logger.isDebugEnabled()) {
                                    logger.debug("GET ALL successful !");
                                }

                            }

                        } catch(Exception e) {
                            getErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    // TODO: Implement this in the next pass
                    case VoldemortOpCode.GET_VERSION_OP_CODE:

                        if(logger.isDebugEnabled()) {
                            logger.debug("Incoming get version request");
                        }

                        try {

                            if(logger.isDebugEnabled()) {
                                logger.debug("GET versions request successful !");
                            }

                        } catch(Exception e) {
                            getVersionErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    case VoldemortOpCode.PUT_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("PUT request received.");
                        }

                        try {
                            VectorClock successfulPutVC = null;

                            if(this.requestObject.getValue() != null) {
                                successfulPutVC = ((VectorClock) this.storeClient.putVersionedWithCustomTimeout(this.requestObject)).clone();
                            } else {
                                successfulPutVC = ((VectorClock) this.storeClient.putWithCustomTimeout(this.requestObject)).clone();
                            }

                            PutResponseSender responseConstructor = new PutResponseSender(messageEvent,
                                                                                          successfulPutVC);
                            responseConstructor.sendResponse();

                            if(logger.isDebugEnabled()) {
                                logger.debug("PUT successful !");
                            }

                        } catch(Exception e) {
                            putErrorHandler.handleExceptions(messageEvent, e);
                        }

                        break;

                    case VoldemortOpCode.DELETE_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("Incoming delete request");
                        }

                        try {
                            boolean isDeleted = this.storeClient.deleteWithCustomTimeout(this.requestObject);
                            if(isDeleted) {
                                // writeResponse();
                                if(logger.isDebugEnabled()) {
                                    logger.debug("DELETE request successful !");
                                }

                            } else {
                                logger.error("Requested Key with the specified version does not exist");
                                RESTErrorHandler.handleError(NOT_FOUND,
                                                             this.messageEvent,
                                                             "Requested Key with the specified version does not exist");
                            }

                        } catch(Exception e) {
                            deleteErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    default:
                        System.err.println("Illegal operation.");
                        return;

                }

            }

        }

    }
}
