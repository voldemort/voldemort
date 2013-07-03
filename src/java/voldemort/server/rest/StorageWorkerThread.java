package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;

import voldemort.common.VoldemortOpCode;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class StorageWorkerThread implements Runnable {

    private final static RestServerGetErrorHandler getErrorHandler = new RestServerGetErrorHandler();
    private final static RestServerDeleteErrorHandler deleteErrorHandler = new RestServerDeleteErrorHandler();
    private final static RestServerGetVersionErrorHandler getVersionErrorHandler = new RestServerGetVersionErrorHandler();
    private final static RestServerPutErrorHandler putErrorHandler = new RestServerPutErrorHandler();
    CompositeVoldemortRequest<ByteArray, byte[]> requestObject;
    private MessageEvent messageEvent;
    private Store<ByteArray, byte[], byte[]> store;

    private final Logger logger = Logger.getLogger(StorageWorkerThread.class);

    public StorageWorkerThread(MessageEvent messageEvent) {
        this.messageEvent = messageEvent;
    }

    @Override
    public void run() {
        Object message = messageEvent.getMessage();
        if(message instanceof VoldemortStoreRequest) {
            VoldemortStoreRequest storeRequestObject = (VoldemortStoreRequest) message;
            this.requestObject = storeRequestObject.getRequestObject();
            this.store = storeRequestObject.getStore();

            switch(requestObject.getOperationType()) {
                case VoldemortOpCode.GET_OP_CODE:
                    if(logger.isDebugEnabled()) {
                        logger.debug("Incoming get request");
                    }
                    try {
                        List<Versioned<byte[]>> versionedValues = store.get(requestObject.getKey(),
                                                                            null);
                        // handle non existing key
                        if(versionedValues.size() > 0) {
                            GetResponseSender responseConstructor = new GetResponseSender(messageEvent,
                                                                                          requestObject.getKey(),
                                                                                          versionedValues,
                                                                                          store.getName());
                            responseConstructor.sendResponse();
                        } else {
                            RestServerErrorHandler.writeErrorResponse(messageEvent,
                                                                      NOT_FOUND,
                                                                      "Key does not exist");
                        }

                    } catch(Exception e) {
                        getErrorHandler.handleExceptions(messageEvent, e);
                    }
                    break;

                case VoldemortOpCode.GET_ALL_OP_CODE:
                    if(logger.isDebugEnabled()) {
                        logger.debug("Incoming get all request");
                    }
                    try {
                        Map<ByteArray, List<Versioned<byte[]>>> keyValuesMap = store.getAll(requestObject.getIterableKeys(),
                                                                                            null);
                        // check if there is atleast one valid key
                        // before sending response
                        boolean hasAtleastOneValidKey = false;
                        for(List<Versioned<byte[]>> values: keyValuesMap.values()) {
                            if(values.size() > 0) {
                                hasAtleastOneValidKey = true;
                                break;
                            }
                        }
                        if(hasAtleastOneValidKey) {
                            GetAllResponseSender responseConstructor = new GetAllResponseSender(messageEvent,
                                                                                                keyValuesMap,
                                                                                                store.getName());
                            responseConstructor.sendResponse();
                        } else {
                            RestServerErrorHandler.writeErrorResponse(messageEvent,
                                                                      NOT_FOUND,
                                                                      "Key does not exist or key is null");
                        }
                    } catch(Exception e) {
                        getErrorHandler.handleExceptions(messageEvent, e);
                    }
                    break;

                case VoldemortOpCode.PUT_OP_CODE:
                    if(logger.isDebugEnabled()) {
                        logger.debug("Incoming put request");
                    }
                    try {
                        store.put(requestObject.getKey(), requestObject.getValue(), null);
                        PutResponseSender responseConstructor = new PutResponseSender(messageEvent);
                        responseConstructor.sendResponse();
                    } catch(Exception e) {
                        putErrorHandler.handleExceptions(messageEvent, e);
                    }
                    break;

                case VoldemortOpCode.DELETE_OP_CODE:
                    if(logger.isDebugEnabled()) {
                        logger.debug("Incoming delete request");
                    }
                    try {
                        boolean result = store.delete(requestObject.getKey(),
                                                      requestObject.getVersion());
                        if(!result) {
                            RestServerErrorHandler.writeErrorResponse(messageEvent,
                                                                      NOT_FOUND,
                                                                      "Non Existing key/version. Nothing to delete");
                            break;
                        }
                        DeleteResponseSender responseConstructor = new DeleteResponseSender(messageEvent);
                        responseConstructor.sendResponse();
                    } catch(Exception e) {
                        deleteErrorHandler.handleExceptions(messageEvent, e);
                    }
                    break;

                case VoldemortOpCode.GET_VERSION_OP_CODE:

                    if(logger.isDebugEnabled()) {
                        logger.debug("Incoming get version request");
                    }
                    try {
                        List<Version> versions = store.getVersions(requestObject.getKey());

                        // handle non existing key
                        if(versions.size() > 0) {
                            GetVersionResponseSender responseConstructor = new GetVersionResponseSender(messageEvent,
                                                                                                        requestObject.getKey(),
                                                                                                        versions,
                                                                                                        store.getName());
                            responseConstructor.sendResponse();

                        } else {
                            RestServerErrorHandler.writeErrorResponse(messageEvent,
                                                                      NOT_FOUND,
                                                                      "Key does not exist or key is null");
                        }
                    } catch(Exception e) {
                        getVersionErrorHandler.handleExceptions(messageEvent, e);
                    }
                    break;
                default:
                    // Since we dont add any other operations than the 5
                    // above, the code stops here.
                    return;
            }
        }

    }

}
