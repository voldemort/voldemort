package voldemort.coordinator;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.client.SocketStoreClientFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.StoreDefinitionUtils;

public class GetSchemataRequestExecutor implements Runnable {

    private SocketStoreClientFactory storeClientFactory;
    private String storeName;
    private final Logger logger = Logger.getLogger(GetSchemataRequestExecutor.class);
    private ChannelBuffer responseContent;
    private MessageEvent getRequestMessageEvent;

    String SCHEMATAJSON = "{\"type\": \"record\", \"name\": \"storeserializer\",\"fields\": ["
                          + "{ \"name\": \"key-serializer\", \"type\": \"string\" } ,"
                          + "{ \"name\": \"value-serializer\", \"type\": \"string\"}]}";

    public GetSchemataRequestExecutor(MessageEvent requestEvent,
                                      String storeName,
                                      SocketStoreClientFactory storeClientFactory) {
        this.storeClientFactory = storeClientFactory;
        this.storeName = storeName;
        this.getRequestMessageEvent = requestEvent;
    }

    @Override
    public void run() {

        StoreDefinition storeDef = StoreDefinitionUtils.getStoreDefinitionWithName(storeClientFactory.getStoreDefs(),
                                                                                   storeName);

        String keySerializer = storeDef.getKeySerializer().toString();
        String valueSerializer = storeDef.getValueSerializer().toString();
        GenericData.Record record = new GenericData.Record(Schema.parse(SCHEMATAJSON));

        record.put("key-serializer", keySerializer);
        record.put("value-serializer", valueSerializer);
        writeResponse(record.toString().getBytes());
    }

    public void writeResponse(byte[] responseValue) {

        this.responseContent = ChannelBuffers.dynamicBuffer(responseValue.length);
        this.responseContent.writeBytes(responseValue);

        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // 2. Set the right headers
        response.setHeader(CONTENT_TYPE, "binary");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");

        // 3. Copy the data into the payload
        response.setContent(responseContent);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        if(logger.isDebugEnabled()) {
            logger.debug("Response = " + response);
        }

        // Write the response to the Netty Channel
        this.getRequestMessageEvent.getChannel().write(response);
    }

}
