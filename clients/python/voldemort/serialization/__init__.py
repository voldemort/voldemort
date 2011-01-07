from common import SerializationException
from json_serializer import JsonTypeSerializer
from string_serializer import StringSerializer
from unimplemented_serializer import UnimplementedSerializer

SERIALIZER_CLASSES = {
    "string": StringSerializer,
    "json": JsonTypeSerializer,
}
