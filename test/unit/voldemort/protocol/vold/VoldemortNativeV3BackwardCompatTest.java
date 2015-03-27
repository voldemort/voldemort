
package voldemort.protocol.vold;

import voldemort.client.protocol.RequestFormatType;

public class VoldemortNativeV3BackwardCompatTest extends VoldemortNativeBackWardCompatTest {

    byte[][][] inputOutputCombination = new byte[][][] {
            // Request Format Type VOLDEMORT_V3
            // Test Get key1, value1, Clock(12345678, 1, 2, 2, 3) , uses Put
            // Underneath
            { // Begin Input
                    new byte[] { 2, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 49,
                            0, 0, 0, 26, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 0, -68,
                            97, 78, 118, 97, 108, 117, 101, 49, 0 }, new byte[] { 0, 0 } }, // End
                                                                                            // Output
            { // Begin Input
                    new byte[] { 1, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 49,
                            0 },
                    new byte[] { 0, 0, 0, 0, 0, 1, 0, 0, 0, 26, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1,
                            0, 0, 0, 0, 0, -68, 97, 78, 118, 97, 108, 117, 101, 49 } }, // End
                                                                                        // Output
            // Test Delete key1, value1, Clock(12345678, 1, 2, 2, 3)
            { // Begin Input
                    new byte[] { 3, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 49,
                            0, 20, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 0, -68, 97, 78 },
                    new byte[] { 0, 0, 1 } }, // End Output
            // Test Get key2, value2, Clock(912345678, 2, 1, 2, 3), uses Put
            // Underneath
            { // Begin Input
                    new byte[] { 2, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 50,
                            0, 0, 0, 26, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 54, 97,
                            74, 78, 118, 97, 108, 117, 101, 50, 0 }, new byte[] { 0, 0 } }, // End
                                                                                            // Output
            { // Begin Input
                    new byte[] { 1, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 50,
                            0 },
                    new byte[] { 0, 0, 0, 0, 0, 1, 0, 0, 0, 26, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1,
                            0, 0, 0, 0, 54, 97, 74, 78, 118, 97, 108, 117, 101, 50 } }, // End
                                                                                        // Output
            // Test Delete key2, value2, Clock(912345678, 2, 1, 2, 3)
            { // Begin Input
                    new byte[] { 3, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 50,
                            0, 20, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 54, 97, 74, 78 },
                    new byte[] { 0, 0, 1 } }, // End Output
            // Test GetVersion key1, value1, Clock(12345678, 1, 2, 2, 3), uses
            // Put Underneath
            { // Begin Input
                    new byte[] { 2, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 49,
                            0, 0, 0, 26, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 0, -68,
                            97, 78, 118, 97, 108, 117, 101, 49, 0 }, new byte[] { 0, 0 } }, // End
                                                                                            // Output
            { // Begin Input
                    new byte[] { 10, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 49 },
                    new byte[] { 0, 0, 0, 0, 0, 1, 0, 0, 0, 20, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1,
                            0, 0, 0, 0, 0, -68, 97, 78 } }, // End Output
            // Test Delete key1, value1, Clock(12345678, 1, 2, 2, 3)
            { // Begin Input
                    new byte[] { 3, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 49,
                            0, 20, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 0, -68, 97, 78 },
                    new byte[] { 0, 0, 1 } }, // End Output
            // Test GetAll key1, value1, Clock(12345678, 1, 2, 2, 3) ,
            // key2, value2, Clock(912345678, 2, 1, 2, 3), uses Put Underneath
            { // Begin Input
                    new byte[] { 2, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 49,
                            0, 0, 0, 26, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 0, -68,
                            97, 78, 118, 97, 108, 117, 101, 49, 0 }, new byte[] { 0, 0 } }, // End
                                                                                            // Output
            { // Begin Input
                    new byte[] { 2, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 4, 107, 101, 121, 50,
                            0, 0, 0, 26, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 54, 97,
                            74, 78, 118, 97, 108, 117, 101, 50, 0 }, new byte[] { 0, 0 } }, // End
                                                                                            // Output
            { // Begin Input
                    new byte[] { 4, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, 107,
                            101, 121, 49, 0, 0, 0, 4, 107, 101, 121, 50, 0 },
                    new byte[] { 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, 107, 101, 121, 50, 0, 0, 0, 1, 0, 0,
                            0, 26, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 54, 97, 74, 78,
                            118, 97, 108, 117, 101, 50, 0, 0, 0, 4, 107, 101, 121, 49, 0, 0, 0, 1,
                            0, 0, 0, 26, 0, 3, 1, 0, 1, 1, 0, 2, 2, 0, 3, 1, 0, 0, 0, 0, 0, -68,
                            97, 78, 118, 97, 108, 117, 101, 49 } }, // End
                                                                    // Output
    };

    @Override
    public byte[][][] getInputOutputCombination() {
        return inputOutputCombination;
    }

    public VoldemortNativeV3BackwardCompatTest() {
        super(RequestFormatType.VOLDEMORT_V3);
    }
}