package voldemort.protocol.vold;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandler;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;


public abstract class VoldemortNativeBackWardCompatTest {


    private final String storeName;
    private final RequestFormat clientWireFormat;
    private final RequestHandler serverWireFormat;
    private final InMemoryStorageEngine<ByteArray, byte[], byte[]> store;

    public VoldemortNativeBackWardCompatTest(RequestFormatType type) {
        this.storeName = "test";
        this.store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeName);
        StoreRepository repository = new StoreRepository();
        repository.addLocalStore(store);
        repository.addRoutedStore(store);
        this.clientWireFormat = new RequestFormatFactory().getRequestFormat(type);
        this.serverWireFormat = ServerTestUtils.getSocketRequestHandlerFactory(repository)
                                               .getRequestHandler(type);
    }

    public abstract byte[][][] getInputOutputCombination();

    @Test
    public void testBackWardCompatibility() throws Exception {
        byte[][][] inputOutputCombination = getInputOutputCombination();
        for(byte[][] inputOuput: inputOutputCombination) {
            byte[] input = inputOuput[0];
            byte[] output = inputOuput[1];
            validateInputOutput(input, output);
        }
    }

    private void validateInputOutput(byte[] input, byte[] output) throws IOException {
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(input));
        ByteArrayOutputStream response = new ByteArrayOutputStream();
        this.serverWireFormat.handleRequest(inputStream, new DataOutputStream(response));

        byte[] responseByteArray = response.toByteArray();
        Assert.assertArrayEquals(output, responseByteArray);
    }
}
