package voldemort.serialization.protobuf;

import junit.framework.TestCase;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

public class ProtoBufSerializerTest extends TestCase {

    private abstract static class AbstractMessageStub extends GeneratedMessage {

        byte[] bytes;

        public AbstractMessageStub(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            throw new UnsupportedOperationException();
        }

        public Message getDefaultInstanceForType() {
            throw new UnsupportedOperationException();
        }

        public com.google.protobuf.Message.Builder newBuilderForType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] toByteArray() {
            return bytes;
        }
    }

    private static class MessageWithNoParseFrom extends AbstractMessageStub {

        public MessageWithNoParseFrom(byte[] bytes) {
            super(bytes);
        }
    }

    private static class InvalidMessageWithParseFrom {

        public static InvalidMessageWithParseFrom parseFrom(@SuppressWarnings("unused") byte[] bytes) {
            return new InvalidMessageWithParseFrom();
        }
    }

    private static class MessageStub extends AbstractMessageStub {

        public MessageStub(byte[] bytes) {
            super(bytes);
        }

        public static MessageStub parseFrom(byte[] bytes) {
            return new MessageStub(bytes);
        }
    }

    public void testValidSchemaInfo() {
        new ProtoBufSerializer<Message>("     java  =   " + MessageStub.class.getName());
        createSerializer();
    }

    public void testToBytes() {
        ProtoBufSerializer<MessageStub> serializer = createSerializer();
        MessageStub message = new MessageStub(new byte[] { 23, 34, 22, 23 });
        assertEquals(message.bytes, serializer.toBytes(message));
    }

    private ProtoBufSerializer<MessageStub> createSerializer() {
        return new ProtoBufSerializer<MessageStub>("java=" + MessageStub.class.getName());
    }

    public void testToObject() {
        ProtoBufSerializer<MessageStub> serializer = createSerializer();
        byte[] bytes = new byte[] { 3, 5, 123 };
        assertEquals(bytes, serializer.toObject(bytes).bytes);
    }

    public void testNonExistentClass() {
        try {
            new ProtoBufSerializer<Message>("java=com.foo.Bar");
        } catch(IllegalArgumentException e) {
            assertEquals(ClassNotFoundException.class, e.getCause().getClass());
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    public void testClassWithNoParseFrom() {
        try {
            new ProtoBufSerializer<Message>("java=" + MessageWithNoParseFrom.class.getName());
        } catch(IllegalArgumentException e) {
            assertEquals(NoSuchMethodException.class, e.getCause().getClass());
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    public void testNoMessageWithParseFrom() {
        try {
            new ProtoBufSerializer<Message>("java=" + InvalidMessageWithParseFrom.class.getName());
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    public void testGetSerializer() {
        SerializerDefinition def = new SerializerDefinition("protobuf",
                                                            "java=" + MessageStub.class.getName());
        Serializer<?> serializer = new DefaultSerializerFactory().getSerializer(def);
        assertEquals(ProtoBufSerializer.class, serializer.getClass());
    }
}
