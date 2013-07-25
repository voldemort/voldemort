package voldemort.coordinator;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;

import voldemort.common.VoldemortOpCode;
import voldemort.server.rest.GetResponseSender;
import voldemort.server.rest.RestServerDeleteErrorHandler;
import voldemort.server.rest.RestServerGetErrorHandler;
import voldemort.server.rest.RestServerGetVersionErrorHandler;
import voldemort.server.rest.RestServerPutErrorHandler;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;
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
    private final Logger logger = Logger.getLogger(getClass());

    public CoordinatorWorkerThread(MessageEvent channelEvent) {
        this.messageEvent = channelEvent;
    }

    @Override
    public void run() {
        Object message = messageEvent.getMessage();
        if(message instanceof CoordinatorStoreClientRequest) {
            CoordinatorStoreClientRequest storeClientRequestObject = (CoordinatorStoreClientRequest) message;
            this.requestObject = storeClientRequestObject.getRequestObject();
            this.storeClient = storeClientRequestObject.getStoreClient();

            // This shouldn't ideally happen.
            if(this.requestObject != null && this.storeClient != null) {

                switch(requestObject.getOperationType()) {
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
                                // writeResponse(versionedResponses);
                                if(logger.isDebugEnabled()) {
                                    logger.debug("GET ALL successful !");
                                }

                            }

                        } catch(Exception e) {
                            getErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    // TODO: Implment this in the next pass
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

                            // Still using the old way here, until the
                            // PutResponseSender is fixed (return a VectorClock
                            // back to the caller)
                            HttpPutRequestExecutor putRequestExecutor = new HttpPutRequestExecutor(this.messageEvent);
                            putRequestExecutor.writeResponse(new VectorClock());

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
