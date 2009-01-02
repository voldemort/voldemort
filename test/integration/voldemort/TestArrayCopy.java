package voldemort;

public class TestArrayCopy {

    /**
     * @param args
     */
    public static void main(String[] args) {

        int iterations = 1000000000;
        int size = 20;

        byte[] source = new byte[size];
        byte[] dest = new byte[size];
        for(int i = 0; i < size; i++)
            source[i] = (byte) i;

        // test arraycopy
        long start = System.nanoTime();
        for(int i = 0; i < iterations; i++)
            System.arraycopy(source, 0, dest, 0, size);
        long ellapsed = System.nanoTime() - start;
        System.out.println("System.arraycopy(): " + (ellapsed / (double) iterations));

        // test for loop
        start = System.nanoTime();
        for(int i = 0; i < iterations; i++)
            for(int j = 0; j < size; j++)
                dest[j] = source[j];
        ellapsed = System.nanoTime() - start;
        System.out.println("for loop: " + (ellapsed / (double) iterations));

    }

}
