package voldemort.store.readonly;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.readonly.chunk.ChunkedFileSet;
import voldemort.utils.ByteUtils;


public class ChunkedFileSetTest {

    private static final int NODE_ID = 0;
    private RoutingStrategy getTempStrategy() {
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < 1; i++) {
            nodes.add(new Node(i, "localhost", 8080 + i, 6666 + i, 7000 + i, Arrays.asList(0)));
        }
        Cluster cluster = new Cluster("test", nodes);

        RoutingStrategy router = new ConsistentRoutingStrategy(cluster, 1);
        return router;
    }

    @Test
    public void testCollision() throws Exception {
        File fileDir = TestUtils.createTempDir();

        File dataFile = new File(fileDir + File.separator + "0_0_0.data");
        dataFile.deleteOnExit();
        
        FileOutputStream fw = new FileOutputStream(dataFile.getAbsoluteFile());
        DataOutputStream outputStream = new DataOutputStream(fw);
        

        /*
         * 4dc968ff0ee35c209572d4777b721587d36fa7b21bdc56b74a3dc0783e7b9518afbfa200a8284bf36e8e4b55b35f427593d849676da0d1555d8360fb5f07fea2
         * and the (different by two bits) 
         * 4dc968ff0ee35c209572d4777b721587d36fa7b21bdc56b74a3dc0783e7b9518afbfa202a8284bf36e8e4b55b35f427593d849676da0d1d55d8360fb5f07fea2
         * both have MD5 hash 008ee33a9d58b51cfeb425b0959121c9
         */

        String[] md5collision = {
                "4dc968ff0ee35c209572d4777b721587d36fa7b21bdc56b74a3dc0783e7b9518afbfa200a8284bf36e8e4b55b35f427593d849676da0d1555d8360fb5f07fea2",
                "4dc968ff0ee35c209572d4777b721587d36fa7b21bdc56b74a3dc0783e7b9518afbfa202a8284bf36e8e4b55b35f427593d849676da0d1d55d8360fb5f07fea2" };
        
        outputStream.writeShort(2);
        for(int i = 0; i < 2; i ++) {
            String input = md5collision[i];
            byte[] hexInput = ByteUtils.fromHexString(input);
            outputStream.writeInt(hexInput.length);
            outputStream.writeInt(hexInput.length);
            outputStream.write(hexInput);
            outputStream.write(hexInput);
        }
        outputStream.close();
        fw.close();


        File indexFile = new File(fileDir + File.separator + "0_0_0.index");
        indexFile.createNewFile();
        indexFile.deleteOnExit();

        ChunkedFileSet fileSet = new ChunkedFileSet(fileDir, 
                                                    getTempStrategy(),
                                                    NODE_ID, 
                                                    VoldemortConfig.DEFAULT_RO_MAX_VALUE_BUFFER_ALLOCATION_SIZE);

        for(int i = 0; i < 2; i ++) {
            String input = md5collision[i];
            byte[] hexInput = ByteUtils.fromHexString(input);
            byte[] hexValue = fileSet.readValue(hexInput, 0, 0);
            Assert.assertArrayEquals(hexInput, hexValue);
        }

    }
}
