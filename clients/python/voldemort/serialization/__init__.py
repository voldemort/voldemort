from common import SerializationException
from json_serializer import JsonTypeSerializer
from string_serializer import StringSerializer

SERIALIZER_CLASSES = {
    "string": StringSerializer,
    "json": JsonTypeSerializer,
}
