package voldemort.performance;

import java.util.ArrayList;
import java.util.List;

import voldemort.serialization.Serializer;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;

public class TestSerialization {

    public static void main(String[] args) {
        JsonTypeDefinition def = JsonTypeDefinition.fromJson("[\"int32\"]");
        int size = 30000;
        List value = new ArrayList(size);
        for(int i = 0; i < size; i++)
            value.add(i);
        Serializer ser = new JsonTypeSerializer(def);
        long start = System.currentTimeMillis();
        int iters = 100;
        List<byte[]> bytes = new ArrayList<byte[]>(iters);
        for(int i = 0; i < iters; i++)
            bytes.add(ser.toBytes(value));
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Took " + (elapsed / iters) + " ms");
    }

}
