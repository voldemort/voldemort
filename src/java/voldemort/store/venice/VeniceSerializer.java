package voldemort.store.venice;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;


/**
 * Venice's custom serialization class. Used by Kafka to convert to/from byte arrays.
 *
 * Message Schema (in order)
 * - Magic Byte
 * - Operation Type
 * - Schema Version
 * - Timestamp
 * - Payload
 *
 */
public class VeniceSerializer implements Encoder<VeniceMessage>, Decoder<VeniceMessage> {

  static final Logger logger = Logger.getLogger(VeniceSerializer.class.getName()); // log4j logger

  private static final int HEADER_LENGTH = 3; // length of the VeniceMessage header in bytes

  public VeniceSerializer(VerifiableProperties verifiableProperties) {
    /* This constructor is not used, but is required for compilation */
  }

  @Override
  /**
   * Converts from a byte array to a VeniceMessage
   * @param byteArray - byte array to be converted
   * @return Converted Venice Message
   * */
  public VeniceMessage fromBytes(byte[] byteArray) {

    byte magicByte;
    byte schemaVersion;
    OperationType operationType = null;
    byte[] output = null;

    ByteArrayInputStream bytesIn = null;
    ObjectInputStream ois = null;

    try {

      bytesIn = new ByteArrayInputStream(byteArray);
      ois = new ObjectInputStream(bytesIn);

      /* read magicByte TODO: currently unused */
      magicByte = ois.readByte();

      /* read operation type */
      byte opTypeByte = ois.readByte();

      switch (opTypeByte) {
        case 1:
          operationType = OperationType.PUT;
          break;
        case 2:
          operationType = OperationType.DELETE;
          break;
        default:
          operationType = null;
          logger.error("Illegal serialized operation type found: " + opTypeByte);
      }

      /* read schemaVersion - TODO: currently unused */
      schemaVersion = ois.readByte();

      /* read payload, one character at a time */
      int byteCount = ois.available();

      output = new byte[byteCount];
      for (int i = 0; i < byteCount; i++) {
        output[i] = ois.readByte();
      }

    } catch (IOException e) {

      logger.error("IOException while performing deserialization: " + e);
      e.printStackTrace();
      return new VeniceMessage(OperationType.ERROR, new byte[0]);

    } finally {

      // safely close the input/output streams
      try { ois.close(); } catch (IOException e) {}
      try { bytesIn.close(); } catch (IOException e) {}

    }

    return new VeniceMessage(operationType, output);

  }

  @Override
  /**
   * Converts from a VeniceMessage to a byte array
   * @param byteArray - byte array to be converted
   * @return Converted Venice Message
   * */
  public byte[] toBytes(VeniceMessage vm) {

    ByteArrayOutputStream bytesOut = null;
    ObjectOutputStream oos = null;
    byte[] message = new byte[0];

    try {

      bytesOut = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(bytesOut);

      oos.writeByte(vm.getMagicByte());

      // serialize the operation type enum
      switch(vm.getOperationType()) {
        case PUT:
          oos.write(1);
          break;
        case DELETE:
          oos.write(2);
          break;
        default:
          logger.error("Operation Type not recognized: " + vm.getOperationType());
          oos.write(0);
          break;
      }

      oos.writeByte(vm.getSchemaVersion());

      // write the payload to the byte array
      oos.write(vm.getPayload());
      oos.flush();

      message = bytesOut.toByteArray();

    } catch (IOException e) {
      logger.error("Could not serialize message: " + vm.getPayload());
    } finally {

      // safely close the input/output streams
      try { oos.close(); } catch (IOException e) {}
      try { bytesOut.close(); } catch (IOException e) {}

    }

    return message;
  }

}
