package voldemort.server.protocol.pb;


import com.google.protobuf.Message;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.VoldemortAdminRequest;
import voldemort.client.protocol.pb.VProto;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.AbstractRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * Created by IntelliJ IDEA.
 * User: afeinber
 * Date: Sep 29, 2009
 * Time: 4:45:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProtoBuffAdminServiceRequestHandler extends AbstractRequestHandler {
    private final Logger logger = Logger.getLogger(ProtoBuffAdminServiceRequestHandler.class);
    private final MetadataStore metadataStore;


    public ProtoBuffAdminServiceRequestHandler(ErrorCodeMapper errorCodeMapper,
                                               StoreRepository storeRepository,
                                               MetadataStore metadataStore) {
        super(errorCodeMapper,storeRepository);
        this.metadataStore = metadataStore;

    }
    
    //@Override
    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream) throws IOException {
        VoldemortAdminRequest.Builder request = ProtoUtils.readToBuilder(inputStream,
                VoldemortAdminRequest.newBuilder());
        Message response;

        switch(request.getType()) {
            case GET_METADATA:
                response = handleGetMetadata(request.getGetMetadata());
                break;
            case UPDATE_METADATA:
                response = handleUpdateMetadata(request.getUpdateMetadata());
                break;
          //  case DELETE_PARTITION_ENTRIES: break;
          //  case FETCH_PARTITION_ENTRIES: break;
          //  case REDIRECT_GET: break;
          //  case UPDATE_PARTITION_ENTRIES: break;
            default:
                throw new VoldemortException("Unkown operation " + request.getType());
        }
        ProtoUtils.writeMessage(outputStream, response);
    }

    public VAdminProto.UpdateMetadataResponse handleUpdateMetadata(VAdminProto.UpdateMetadataRequest request) {
        VAdminProto.UpdateMetadataResponse.Builder response = VAdminProto.UpdateMetadataResponse.newBuilder();

        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            String keyString = ByteUtils.getString(key.get(), "UTF-8");

            if (MetadataStore.METADATA_KEYS.contains(keyString)) {
                Versioned<byte[]> versionedValue = ProtoUtils.decodeVersioned(request.getVersioned());
                metadataStore.put(new ByteArray(ByteUtils.getBytes(keyString, "UTF-8")), versionedValue);
            }
            

        } catch (VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }

        return response.build();
    }

    public VAdminProto.GetMetadataResponse handleGetMetadata(VAdminProto.GetMetadataRequest request) {
        VAdminProto.GetMetadataResponse.Builder response = VAdminProto.GetMetadataResponse.newBuilder();

        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            String keyString = ByteUtils.getString(key.get(), "UTF-8");

            if (MetadataStore.METADATA_KEYS.contains(keyString)) {
                List<Versioned<byte[]>> versionedList =
                        metadataStore.get(key);
                int size = (versionedList.size() > 0) ? 1 : 0;
                if (size > 0) {
                    Versioned<byte[]> versioned = versionedList.get(0);
                    response.setVersion(ProtoUtils.encodeVersioned(versioned));
                }
            } else {
                throw new VoldemortException("Metadata Key passed " + keyString + " is not handled yet ...");

            }
        } catch (VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        return response.build();

    }
    /**
     * This method is used by non-blocking code to determine if the give buffer
     * represents a complete request. Because the non-blocking code can by
     * definition not just block waiting for more data, it's possible to get
     * partial reads, and this identifies that case.
     *
     * @param buffer Buffer to check; the buffer is reset to position 0 before
     *               calling this method and the caller must reset it after the call
     *               returns
     * @return True if the buffer holds a complete request, false otherwise
     */
    //@Override
    public boolean isCompleteRequest(ByteBuffer buffer) {
        throw new VoldemortException("Non-blocking server not supported for ProtoBuffAdminServiceRequestHandler");
    }
}
