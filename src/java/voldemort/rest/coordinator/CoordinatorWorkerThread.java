package voldemort.rest.coordinator;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;

import voldemort.common.VoldemortOpCode;
import voldemort.rest.DeleteResponseSender;
import voldemort.rest.GetAllResponseSender;
import voldemort.rest.GetMetadataResponseSender;
import voldemort.rest.GetResponseSender;
import voldemort.rest.PutResponseSender;
import voldemort.rest.RestDeleteErrorHandler;
import voldemort.rest.RestErrorHandler;
import voldemort.rest.RestGetErrorHandler;
import voldemort.rest.RestGetVersionErrorHandler;
import voldemort.rest.RestPutErrorHandler;
import voldemort.rest.RestUtils;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.StoreDefinition;
import voldemort.store.stats.StoreStats;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class CoordinatorWorkerThread implements Runnable {

    private final static RestGetErrorHandler getErrorHandler = new RestGetErrorHandler();
    private final static RestGetVersionErrorHandler getVersionErrorHandler = new RestGetVersionErrorHandler();
    private final static RestPutErrorHandler putErrorHandler = new RestPutErrorHandler();
    private final static RestDeleteErrorHandler deleteErrorHandler = new RestDeleteErrorHandler();

    private MessageEvent messageEvent;
    CompositeVoldemortRequest<ByteArray, byte[]> requestObject;
    private DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient = null;
    private final CoordinatorMetadata coordinatorMetadata;
    private final Logger logger = Logger.getLogger(getClass());
    private final StoreStats coordinatorPerfStats;

    public CoordinatorWorkerThread(MessageEvent channelEvent,
                                   CoordinatorMetadata coordinatorMetadata,
                                   StoreStats coordinatorPerfStats) {
        this.messageEvent = channelEvent;
        this.coordinatorMetadata = coordinatorMetadata;
        this.coordinatorPerfStats = coordinatorPerfStats;
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
                            String serializerInfoXml = RestUtils.constructSerializerInfoXml(storeDef);
                            GetMetadataResponseSender metadataResponseSender = new GetMetadataResponseSender(messageEvent,
                                                                                                             serializerInfoXml.getBytes());

                            metadataResponseSender.sendResponse(this.coordinatorPerfStats,
                                                                true,
                                                                this.requestObject.getRequestOriginTimeInMs());
                            if(logger.isDebugEnabled()) {
                                logger.debug("GET Metadata successful !");
                            }
                        } catch(Exception e) {
                            /*
                             * We might get InsufficientOperationalNodes
                             * exception due to a timeout, thus creating
                             * confusion in the root cause. Hence explicitly
                             * check for timeout.
                             */
                            if(System.currentTimeMillis() >= (this.requestObject.getRequestOriginTimeInMs() + this.requestObject.getRoutingTimeoutInMs())) {
                                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                                    REQUEST_TIMEOUT,
                                                                    "GET METADATA request timed out: "
                                                                            + e.getMessage());
                            }

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
                                responseConstructor.sendResponse(this.coordinatorPerfStats,
                                                                 true,
                                                                 this.requestObject.getRequestOriginTimeInMs());
                                if(logger.isDebugEnabled()) {
                                    logger.debug("GET successful !");
                                }

                            } else {
                                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                                    NOT_FOUND,
                                                                    "Requested Key does not exist");
                            }

                        } catch(Exception e) {
                            /*
                             * We might get InsufficientOperationalNodes
                             * exception due to a timeout, thus creating
                             * confusion in the root cause. Hence explicitly
                             * check for timeout.
                             */
                            if(System.currentTimeMillis() >= (this.requestObject.getRequestOriginTimeInMs() + this.requestObject.getRoutingTimeoutInMs())) {
                                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                                    REQUEST_TIMEOUT,
                                                                    "GET request timed out: "
                                                                            + e.getMessage());
                            }

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

                                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                                    NOT_FOUND,
                                                                    "Error when doing getall. Keys do not exist.");
                            } else {
                                GetAllResponseSender responseConstructor = new GetAllResponseSender(messageEvent,
                                                                                                    versionedResponses,
                                                                                                    this.storeClient.getStoreName());
                                responseConstructor.sendResponse(this.coordinatorPerfStats,
                                                                 true,
                                                                 this.requestObject.getRequestOriginTimeInMs());

                                if(logger.isDebugEnabled()) {
                                    logger.debug("GET ALL successful !");
                                }

                            }

                        } catch(Exception e) {
                            /*
                             * We might get InsufficientOperationalNodes
                             * exception due to a timeout, thus creating
                             * confusion in the root cause. Hence explicitly
                             * check for timeout.
                             */
                            if(System.currentTimeMillis() >= (this.requestObject.getRequestOriginTimeInMs() + this.requestObject.getRoutingTimeoutInMs())) {
                                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                                    REQUEST_TIMEOUT,
                                                                    "GET ALL request timed out: "
                                                                            + e.getMessage());
                            }

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
                            /*
                             * We might get InsufficientOperationalNodes
                             * exception due to a timeout, thus creating
                             * confusion in the root cause. Hence explicitly
                             * check for timeout.
                             */
                            if(System.currentTimeMillis() >= (this.requestObject.getRequestOriginTimeInMs() + this.requestObject.getRoutingTimeoutInMs())) {
                                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                                    REQUEST_TIMEOUT,
                                                                    "GET VERSION request timed out: "
                                                                            + e.getMessage());
                            }

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
                                                                                          successfulPutVC,
                                                                                          this.storeClient.getStoreName(),
                                                                                          this.requestObject.getKey());
                            responseConstructor.sendResponse(this.coordinatorPerfStats,
                                                             true,
                                                             this.requestObject.getRequestOriginTimeInMs());

                            if(logger.isDebugEnabled()) {
                                logger.debug("PUT successful !");
                            }

                        } catch(Exception e) {
                            /*
                             * We might get InsufficientOperationalNodes
                             * exception due to a timeout, thus creating
                             * confusion in the root cause. Hence explicitly
                             * check for timeout.
                             */
                            if(System.currentTimeMillis() >= (this.requestObject.getRequestOriginTimeInMs() + this.requestObject.getRoutingTimeoutInMs())) {
                                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                                    REQUEST_TIMEOUT,
                                                                    "PUT request timed out: "
                                                                            + e.getMessage());
                            }

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
                                DeleteResponseSender responseConstructor = new DeleteResponseSender(messageEvent,
                                                                                                    this.storeClient.getStoreName(),
                                                                                                    this.requestObject.getKey());
                                responseConstructor.sendResponse(this.coordinatorPerfStats,
                                                                 true,
                                                                 this.requestObject.getRequestOriginTimeInMs());

                                if(logger.isDebugEnabled()) {
                                    logger.debug("DELETE request successful !");
                                }

                            } else {
                                logger.error("Requested Key with the specified version does not exist");
                                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                                    NOT_FOUND,
                                                                    "Requested Key with the specified version does not exist");
                            }

                        } catch(Exception e) {

                            /*
                             * We might get InsufficientOperationalNodes
                             * exception due to a timeout, thus creating
                             * confusion in the root cause. Hence explicitly
                             * check for timeout.
                             */
                            if(System.currentTimeMillis() >= (this.requestObject.getRequestOriginTimeInMs() + this.requestObject.getRoutingTimeoutInMs())) {
                                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                                    REQUEST_TIMEOUT,
                                                                    "DELETE request timed out: "
                                                                            + e.getMessage());
                            }

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
