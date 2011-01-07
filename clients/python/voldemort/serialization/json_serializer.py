from common import *

import cStringIO as StringIO
import datetime
import struct
import simplejson
from xml.dom import minidom

# get the real OrderedDict if we're on 2.7
try:
    from collections import OrderedDict
except ImportError, e:
    from ordered_dict import OrderedDict


TYPES = {
    "int8": (int, long),
    "int16": (int, long),
    "int32": (int, long),
    "int64": (int, long),
    "float32": (float,),
    "float64": (float,),
    "boolean": (bool,),
    "string": (str, unicode),
    "bytes": (str,),
    "date": (datetime.datetime,),
}

MAXES = {
    "int8": (1 << 7) - 1,
    "int16": (1 << 15) - 1,
    "int32": (1 << 31) - 1,
    "int64": (1 << 63) - 1,
    "float32": struct.unpack(">f", "\x7f\x7f\xff\xff")[0],
    "float64": struct.unpack(">d", "\x7f\xef\xff\xff\xff\xff\xff\xff")[0],
}

MINS = {
    "int8": -(1 << 7),
    "int16": -(1 << 15),
    "int32": -(1 << 31),
    "int64": -(1 << 63),
    "float32": struct.unpack(">f", "\x00\x00\x00\x01")[0],
    "float64": struct.unpack(">d", "\x00\x00\x00\x00\x00\x00\x00\x01")[0],
}

def _is_valid_float(f, typedef):
    return abs(f) <= MAXES[typedef] and (f == 0.0 or abs(f) > MINS[typedef])

def _is_valid_int(i, typedef):
    return i > MINS[typedef] and i <= MAXES[typedef]

RANGE_FNS = {
    "int8": _is_valid_int,
    "int16": _is_valid_int,
    "int32": _is_valid_int,
    "int64": _is_valid_int,
    "float32": _is_valid_float,
    "float64": _is_valid_float,
}

FORMATS = {
    "int8": ">b",
    "int16": ">h",
    "int32": ">i",
    "int64": ">q",
    "float32": ">f",
    "float64": ">d",
}

MAX_SEQ_LENGTH = 0x3FFFFFFF

EPOCH = datetime.datetime(1970, 1, 1, 0, 0, 0, 0)
def _to_java_date(date):
    """
    Converts a python datetime into an int64 representing milliseconds since
    00:00:00 1/1/1970 GMT. This is the expected input to the Java Date class.
    Microseconds in the Python datetime representation are truncated.

    >>> d = datetime.datetime(2010, 11, 24, 11, 50, 34, 237861)
    >>> _to_java_date(d)
    1290599434237

    >>> _from_java_date(_to_java_date(d))
    datetime.datetime(2010, 11, 24, 11, 50, 34, 237000)
    """

    td = date - EPOCH
    ms = td.days * 24 * 3600 * 1000 + td.seconds * 1000 + td.microseconds / 1000
    return ms


def _from_java_date(javaDate):
    """
    Converts an int64 representing a Java Date as milliseconds since
    00:00:00 1/1/1970 GMT into a Python datetime.

    >>> _from_java_date(1000000000000)
    datetime.datetime(2001, 9, 9, 1, 46, 40)

    >>> _to_java_date(_from_java_date(1000000000000))
    1000000000000
    """

    td = datetime.timedelta(microseconds = javaDate * 1000)
    return EPOCH + td


class JsonTypeSerializer(object):
    """
    Python implementation of the Voldemort JsonTypeSerializer class, which converts structured
    types consisting of lists, dicts, and primitive types (strings, ints, floats, dates) into
    the proprietary binary representation used by Voldemort.
    """

    def __init__(self, typedef, has_version=False):
        r"""
        Constructor. The typedef is either a json string containing the schema definition or a
        map from integer version number to schema json.

        A simple json schema results in a non-versioned serializer (note that the output is
        only the 4-byte binary value WITHOUT a version number prefix:

        >>> s = JsonTypeSerializer('"int32"')
        >>> s.writes(42)
        '\x00\x00\x00*'

        A dict typedef will always yield a versioned serializer (note that now there is a version
        number byte appended to the output:

        >>> s = JsonTypeSerializer({1: '"int32"'})
        >>> s.writes(42)
        '\x01\x00\x00\x00*'

        Setting has_version=True will also return a versioned serializer, with an implied version 0:

        >>> s = JsonTypeSerializer('"int32"', has_version=True)
        >>> s.writes(42)
        '\x00\x00\x00\x00*'

        """

        self._has_version = has_version or isinstance(typedef, dict)

        if not isinstance(typedef, dict):
            typeobj = simplejson.loads(typedef, object_pairs_hook=OrderedDict)
            if self._has_version:
                self._typedef = dict([(0, typeobj)])
            else:
                self._typedef = typeobj
        else:
            self._typedef = dict((k, simplejson.loads(v, object_pairs_hook=OrderedDict))
                                for k, v in typedef.iteritems())


    @staticmethod
    def create_from_xml(node):
        r"""
        Static factory method that creates a serializer from then XML description in a
        stores.xml file.

        >>> from xml.dom import minidom
        >>> xml = minidom.parseString(
        ...   '<serializer><type>json</type><schema-info version="0">"int32"</schema-info></serializer>')
        >>> s = JsonTypeSerializer.create_from_xml(xml)
        >>> s.writes(42)
        '\x00\x00\x00\x00*'

        Output always uses the latest version:
        >>> xml = minidom.parseString(
        ...         '<serializer><type>json</type>' +
        ...                     '<schema-info version="0">"int32"</schema-info>' +
        ...                     '<schema-info version="1">"int64"</schema-info></serializer>')
        >>> s = JsonTypeSerializer.create_from_xml(xml)
        >>> s.writes(42)
        '\x01\x00\x00\x00\x00\x00\x00\x00*'

        Duplicate version numbers are an error:
        >>> xml = minidom.parseString(
        ...         '<serializer><type>json</type>' +
        ...                     '<schema-info version="0">"int32"</schema-info>' +
        ...                     '<schema-info version="0">"int64"</schema-info></serializer>')
        >>> s = JsonTypeSerializer.create_from_xml(xml)
        Traceback (most recent call last):
        ...
        SerializationException: Schema info has duplicates of version: 0

        The version "none" means no versioning, and no version will be output in the byte stream:
        >>> xml = minidom.parseString(
        ...   '<serializer><type>json</type><schema-info version="none">"int32"</schema-info></serializer>')
        >>> s = JsonTypeSerializer.create_from_xml(xml)
        >>> s.writes(42)
        '\x00\x00\x00*'

        You can't mix a "none" version with a regular version number:
        >>> xml = minidom.parseString(
        ...         '<serializer><type>json</type>' +
        ...                     '<schema-info version="none">"int32"</schema-info>' +
        ...                     '<schema-info version="0">"int64"</schema-info></serializer>')
        >>> s = JsonTypeSerializer.create_from_xml(xml)
        Traceback (most recent call last):
        ...
        SerializationException: Schema info has duplicates of version: 0
        """

        typedef = dict()

        has_version = True
        for schema_info in node.getElementsByTagName('schema-info'):
            version = schema_info.getAttribute('version')
            if not version:
                version = 0
            elif version == 'none':
                version = 0
                has_version = False
            else:
                version = int(version)

            if version in typedef:
                raise SerializationException('Schema info has duplicates of version: %d' % version)

            typedef[version] = ''.join(elem.data for elem in schema_info.childNodes
                                       if elem.nodeType == minidom.Node.TEXT_NODE)

        if not typedef:
            raise SerializationException('No schemas specified')

        if not has_version and len(typedef) > 1:
            raise SerializationException('Schema info has version="none" and multiple versions')

        if not has_version:
            return JsonTypeSerializer(typedef[0])
        else:
            return JsonTypeSerializer(typedef)


    def read(self, input):
        r"""
        Reads a serialized object from the file-like object input:

        >>> f = StringIO.StringIO('\x00\x00\x00*')
        >>> s = JsonTypeSerializer('"int32"')
        >>> s.read(f)
        42

        >>> versioned = '\00\x01>\x80\x00\x00\x00\x03\x00\x01\x00\x02\x00\x03\x00\x03foo'
        >>> non_versioned = '\x01>\x80\x00\x00\x00\x03\x00\x01\x00\x02\x00\x03\x00\x03foo'

        More complex types are also supported:
        >>> s = JsonTypeSerializer('{ "a":"float32", "b":["int16"], "c":"string" }')
        >>> f = StringIO.StringIO(non_versioned)
        >>> s.read(f) == {'a': 0.25, 'b': [1, 2, 3], 'c':u'foo'}
        True

        Non-versioned serializers can't read versioned binary representations:

        >>> f = StringIO.StringIO(versioned)
        >>> s.read(f)
        Traceback (most recent call last):
        ...
        SerializationException: Unexpected end of input.

        And vice-version, versioned serializers will only read versioned binary representations:

        >>> s = JsonTypeSerializer('{ "a":"float32", "b":["int16"], "c":"string" }', has_version=True)
        >>> f = StringIO.StringIO(versioned)
        >>> s.read(f) == {'a': 0.25, 'b': [1, 2, 3], 'c': u'foo'}
        True

        >>> f = StringIO.StringIO(non_versioned)
        >>> s.read(f)
        Traceback (most recent call last):
        ...
        KeyError: 1

        The error messages from reading improperly versioned representations aren't super helpful...
        """

        if self._has_version:
            version = self._read_int8(input)
            typedef = self._typedef[version]
        else:
            typedef = self._typedef

        return self._read(input, typedef)

    def reads(self, s):
        r"""
        Reads a serialized object from given string:

        >>> s = JsonTypeSerializer('"int32"')
        >>> s.reads('\x00\x00\x00*')
        42

        Same rules as read()/write() with regard to versioned representations apply to reads()/writes():

        >>> versioned = '\00\x01>\x80\x00\x00\x00\x03\x00\x01\x00\x02\x00\x03\x00\x03foo'
        >>> non_versioned = '\x01>\x80\x00\x00\x00\x03\x00\x01\x00\x02\x00\x03\x00\x03foo'

        >>> s = JsonTypeSerializer('{ "a":"float32", "b":["int16"], "c":"string" }')
        >>> s.reads(non_versioned) == {'a': 0.25, 'c': u'foo', 'b': [1, 2, 3]}
        True

        >>> s.reads(versioned)
        Traceback (most recent call last):
        ...
        SerializationException: Unexpected end of input.

        >>> s = JsonTypeSerializer('{ "a":"float32", "b":["int16"], "c":"string" }', has_version=True)
        >>> s.reads(versioned) == {'a': 0.25, 'b': [1, 2, 3], 'c': u'foo'}
        True

        >>> s.reads(non_versioned)
        Traceback (most recent call last):
        ...
        KeyError: 1
        """

        return self.read(StringIO.StringIO(s))

    def write(self, output, obj):
        r"""
        Writes the serialized binary representation of an object to the file-like object output:

        >>> s = JsonTypeSerializer('{ "a":"float32", "b":["int16"], "c":"string" }')
        >>> f = StringIO.StringIO()
        >>> s.write(f, {'a': 0.25, 'b':[1,2,3], 'c':'foo'})
        >>> f.getvalue()
        '\x01>\x80\x00\x00\x00\x03\x00\x01\x00\x02\x00\x03\x00\x03foo'

        The representation of a versioned serializer will have the version number byte prefix:
        >>> s = JsonTypeSerializer('{ "a":"float32", "b":["int16"], "c":"string" }', has_version=True)
        >>> f = StringIO.StringIO()
        >>> s.write(f, {'a': 0.25, 'b':[1,2,3], 'c':'foo'})
        >>> f.getvalue()
        '\x00\x01>\x80\x00\x00\x00\x03\x00\x01\x00\x02\x00\x03\x00\x03foo'
        """

        if self._has_version:
            latest = max(self._typedef.keys())
            typedef = self._typedef[latest]
            self._write_int8(output, latest)
        else:
            typedef = self._typedef

        self._write(output, obj, typedef)

    def writes(self, obj):
        r"""
        Returns a string representing the serialized binary representation of obj:

        >>> s = JsonTypeSerializer('{ "a":"float32", "b":["int16"], "c":"string" }')
        >>> s.writes({'a': 0.25, 'b':[1,2,3], 'c':'foo'})
        '\x01>\x80\x00\x00\x00\x03\x00\x01\x00\x02\x00\x03\x00\x03foo'

        The representation of a versioned serializer will have the version number byte prefix:
        >>> s = JsonTypeSerializer('{ "a":"float32", "b":["int16"], "c":"string" }', has_version=True)
        >>> s.writes({'a': 0.25, 'b':[1,2,3], 'c':'foo'})
        '\x00\x01>\x80\x00\x00\x00\x03\x00\x01\x00\x02\x00\x03\x00\x03foo'

        reads() and writes() are more or less inverses of each other:

        >>> s = JsonTypeSerializer('"int32"')
        >>> s.reads(s.writes(42))
        42

        Strings may be converted to unicodes:
        >>> s = JsonTypeSerializer('"string"')
        >>> s.reads(s.writes('foo'))
        u'foo'

        Dates may lose some precision:
        >>> s = JsonTypeSerializer('"date"')
        >>> s.reads(s.writes(datetime.datetime(2010, 11, 24, 11, 50, 34, 237861)))
        datetime.datetime(2010, 11, 24, 11, 50, 34, 237000)

        Nested types also work:
        >>> s = JsonTypeSerializer('{ "a":"float32", "b":["int16"], "c":"string" }')
        >>> s.reads(s.writes({'a': 0.25, 'b':[1,2,3], 'c':'foo'})) == {'a': 0.25, 'b':[1,2,3], 'c':u'foo'}
        True
        """

        sfile = StringIO.StringIO()
        self.write(sfile, obj)
        return sfile.getvalue()

    def _read(self, input, typedef):
        r"""
        Internal routine for reading a complex type:

        >>> s = JsonTypeSerializer('"int32"')
        >>> f = StringIO.StringIO('\x01>\x80\x00\x00\x00\x03\x00\x01\x00\x02\x00\x03\x00\x03foo')
        >>> obj = s._read(f, OrderedDict((('a','float32'), ('b',['int16']), ('c','string'))))
        >>> obj == {'a': 0.25, 'c': u'foo', 'b': [1, 2, 3]}
        True
        """

        if isinstance(typedef, dict):
            return self._read_dict(input, typedef)
        elif isinstance(typedef, list):
            return self._read_list(input, typedef)
        elif isinstance(typedef, str) or isinstance(typedef, unicode):
            if typedef not in TYPES.keys():
                raise SerializationException("Unknown type string: %s" % typedef)

            return getattr(self, "_read_%s" % typedef)(input)
        else:
            raise SerializationException("Unexpected type: %s" % type(typedef))

    def _read_boolean(self, input):
        r"""
        Internal routine for reading booleans:

        >>> s = JsonTypeSerializer('"int32"')
        >>> s._read_boolean(StringIO.StringIO('\x00'))
        False

        >>> s._read_boolean(StringIO.StringIO('\x01'))
        True

        Negative int8s indicate "None":

        >>> s._read_boolean(StringIO.StringIO('\xff')) is None
        True

        Positive int8s are treated as True, even though _write_boolean() will always write \x01:

        >>> s._read_boolean(StringIO.StringIO('\x05'))
        True
        """

        b = self._read_int8(input)
        if b < 0:
            return None
        elif b == 0:
            return False
        else:
            return True

    def _read_numeric(self, input, typedef):
        r"""
        Internal routine for reading numeric types:

        >>> s = JsonTypeSerializer('"string"')

        >>> s._read_numeric(StringIO.StringIO('*'), 'int8')
        42
        >>> s._read_numeric(StringIO.StringIO('\x00*'), 'int16')
        42
        >>> s._read_numeric(StringIO.StringIO('\x00\x00\x00*'), 'int32')
        42
        >>> s._read_numeric(StringIO.StringIO('\x00\x00\x00\x00\x00\x00\x00*'), 'int64')
        42

        >>> s._read_numeric(StringIO.StringIO('>\x80\x00\x00'), 'float32')
        0.25
        >>> s._read_numeric(StringIO.StringIO('?\xd0\x00\x00\x00\x00\x00\x00'), 'float64')
        0.25

        Inputs corresponding to the smallest of each respective type are read as None:

        >>> s._read_numeric(StringIO.StringIO('\x80'), 'int8') is None
        True
        >>> s._read_numeric(StringIO.StringIO('\x80\x00'), 'int16') is None
        True
        >>> s._read_numeric(StringIO.StringIO('\x80\x00\x00\x00'), 'int32') is None
        True
        >>> s._read_numeric(StringIO.StringIO('\x80\x00\x00\x00\x00\x00\x00\x00'), 'int64') is None
        True

        >>> s._read_numeric(StringIO.StringIO('\x00\x00\x00\x01'), 'float32') is None
        True
        >>> s._read_numeric(StringIO.StringIO('\x00\x00\x00\x00\x00\x00\x00\x01'), 'float64') is None
        True

        An insufficiently large input will cause an error:
        >>> s._read_numeric(StringIO.StringIO('\x00*'), 'int32')
        Traceback (most recent call last):
        ...
        SerializationException: Unexpected end of input.

        An excessively large one will leave dangling input, which may cause problems down the line
        as well as returning the wrong value:
        >>> s._read_numeric(StringIO.StringIO('\x00\x00\x00*'), 'int16')
        0

        >>> s._read_numeric(StringIO.StringIO('?\xd0\x00\x00\x00\x00\x00\x00'), 'float32') == 0.25
        False
        """

        size = struct.calcsize(FORMATS[typedef])
        bytes = input.read(size)
        if len(bytes) < size:
            raise SerializationException("Unexpected end of input.")
        val = struct.unpack(FORMATS[typedef], bytes)[0]
        if val == MINS[typedef]:
            return None
        return val

    # This is totally broken for length values 0x3fff0000-0x3fffffff, the true max length is 0x3ffeffff,
    # but as written, this is a pure port of the java deserialization stuff
    def _read_length(self, input):
        r"""
        Internal routines that read Voldemort's screwy variable length encoding:

        >>> s = JsonTypeSerializer('"string"')

        An int16 -1 value is treated as -1:
        >>> s._read_length(StringIO.StringIO('\xff\xff'))
        -1

        A positive int16 is treated as a short length:
        >>> s._read_length(StringIO.StringIO('\x00\x05'))
        5

        A negative int16 in the first two bytes is a signal that it's really an int32 length:
        >>> s._read_length(StringIO.StringIO('\xff\x00\x00\x00'))
        1056964608

        But any 32-bit length between 0x3fff0000-0x3fffffff will be misinterpreted as -1 (this
        is a bug, but it's identical behavior to the java version):
        >>> s._read_length(StringIO.StringIO('\xff\xff\x00\x05'))
        -1
        """

        firstWord = self._read_int16(input)
        if firstWord == -1:
            return -1

        if firstWord < -1:
            secondWord = self._read_int16(input)
            return ((firstWord & 0x3FFF) << 16) + (secondWord & 0xFFFF)

        return firstWord

    def _read_bytes(self, input):
        r"""
        Internal routine for reading raw bytes strings. The length is encoded at the start
        of the string:

        >>> s = JsonTypeSerializer('"string"')

        >>> s._read_bytes(StringIO.StringIO('\x00\x03foo'))
        'foo'

        A length of 0 is the empty string:
        >>> s._read_bytes(StringIO.StringIO('\x00\x00'))
        ''

        A negative length is None:
        >>> s._read_bytes(StringIO.StringIO('\xff\xff'))

        We get an error if the data is too short:
        >>> s._read_bytes(StringIO.StringIO('\x00\x0afoo'))
        Traceback (most recent call last):
        ...
        SerializationException: Unexpected end of input.
        """

        size = self._read_length(input)
        if size < 0:
            return None

        if size == 0:
            return ''

        bytes = input.read(size)
        if len(bytes) != size:
            raise SerializationException("Unexpected end of input.")
        return bytes

    def _read_string(self, input):
        r"""
        Internal routine for reading UTF-8 encoded strings.

        >>> s = JsonTypeSerializer('"string"')

        >>> s._read_string(StringIO.StringIO('\x00\x03foo'))
        u'foo'

        A length of 0 is the empty string:
        >>> s._read_string(StringIO.StringIO('\x00\x00'))
        u''

        A negative length is None:
        >>> s._read_string(StringIO.StringIO('\xff\xff'))

        We get an error if the data is too short:
        >>> s._read_string(StringIO.StringIO('\x00\x0afoo'))
        Traceback (most recent call last):
        ...
        SerializationException: Unexpected end of input.
        """

        bytes = self._read_bytes(input)
        if bytes is None:
            return None

        if not bytes:
            return u''

        return str.decode(bytes, "utf_8")

    def _read_date(self, input):
        r"""
        Internal routine that reads a date:

        >>> s = JsonTypeSerializer('"string"')
        >>> s._read_date(StringIO.StringIO('\x00\x00\x01,~\x90\x84\x82'))
        datetime.datetime(2010, 11, 24, 15, 46, 29, 122000)

        The byte string corresponding to the smallest int64 deserializes to None:
        >>> s._read_date(StringIO.StringIO('\x80\x00\x00\x00\x00\x00\x00\x00'))
        """

        javaDate = self._read_int64(input)
        if javaDate is None:
            return None
        return _from_java_date(javaDate)

    def _read_list(self, input, typedef):
        r"""
        Internal routine for reading lists:

        >>> s = JsonTypeSerializer('"string"')
        >>> s._read_list(StringIO.StringIO('\x00\x03\x00\x01\x00\x02\x00\x03'), ['int16'])
        [1, 2, 3]

        List typedefs must be singleton lists:
        >>> s._read_list(StringIO.StringIO('\x00\x03\x00\x01\x00\x02\x00\x03'), [])
        Traceback (most recent call last):
        ...
        SerializationException: Expected single element typedef, but got: 0

        >>> s._read_list(StringIO.StringIO('\x00\x03\x00\x01\x00\x02\x00\x03'), ['int16', 'int32'])
        Traceback (most recent call last):
        ...
        SerializationException: Expected single element typedef, but got: 2

        >>> s._read_list(StringIO.StringIO('\x00\x03\x00\x01\x00\x02\x00\x03'), 'int16')
        Traceback (most recent call last):
        ...
        SerializationException: Wrong type: expected list but got: <type 'str'>

        A length of -1 return None:
        >>> s._read_list(StringIO.StringIO('\xff\xff'), ['int16']) is None
        True

        A length of 0 is an empty list:
        >>> s._read_list(StringIO.StringIO('\x00\x00'), ['int16'])
        []
        """

        if not isinstance(typedef, list):
            raise SerializationException("Wrong type: expected list but got: %s" % type(typedef))

        if len(typedef) != 1:
            raise SerializationException("Expected single element typedef, but got: %d" % len(typedef))

        size = self._read_length(input)
        if size < 0:
            return None

        entryType = typedef[0]
        return [self._read(input, entryType) for i in xrange(0, size)]

    def _read_dict(self, input, typedef):
        r"""
        Internal routine for reading dicts:

        >>> s = JsonTypeSerializer('"string"')
        >>> obj = s._read_dict(StringIO.StringIO('\x01\x00\x01\x00\x02'), OrderedDict((('a','int16'), ('b','int16'))))
        >>> obj == {'a': 1, 'b': 2}
        True

        Typedef has to be a map:
        >>> s._read_dict(StringIO.StringIO('\x01\x00\x01\x00\x02'), ['int16'])
        Traceback (most recent call last):
        ...
        SerializationException: Wrong typedef type: expected dict but got: <type 'list'>


        If the serialized blob starts with (int8) -1, then the map is read as None:
        >>> s._read_dict(StringIO.StringIO('\xff'), OrderedDict((('a','int16'), ('b','int16')))) is None
        True
        """

        if self._read_int8(input) == -1:
            return None

        if not isinstance(typedef, dict):
            raise SerializationException("Wrong typedef type: expected dict but got: %s" % type(typedef))

        m = {}
        for key, entrytype in typedef.iteritems():
            m[key] = self._read(input, entrytype)

        return m

    def _write(self, output, obj, typedef):
        r"""
        Internal routine that serializes objects according to the passed in typedef.

        >>> s = JsonTypeSerializer('"string"')
        >>> f = StringIO.StringIO()
        >>> s._write(f, True, 'boolean')
        >>> f.getvalue()
        '\x01'

        >>> f = StringIO.StringIO()
        >>> s._write(f, 42, 'int8')
        >>> f.getvalue()
        '*'

        >>> f = StringIO.StringIO()
        >>> s._write(f, 42, 'int16')
        >>> f.getvalue()
        '\x00*'

        >>> f = StringIO.StringIO()
        >>> s._write(f, 42, 'int32')
        >>> f.getvalue()
        '\x00\x00\x00*'

        >>> f = StringIO.StringIO()
        >>> s._write(f, 42, 'int64')
        >>> f.getvalue()
        '\x00\x00\x00\x00\x00\x00\x00*'

        >>> f = StringIO.StringIO()
        >>> s._write(f, 0.25, 'float32')
        >>> f.getvalue()
        '>\x80\x00\x00'

        >>> f = StringIO.StringIO()
        >>> s._write(f, 0.25, 'float64')
        >>> f.getvalue()
        '?\xd0\x00\x00\x00\x00\x00\x00'

        >>> f = StringIO.StringIO()
        >>> s._write(f, [1,2,3], ['int16'])
        >>> f.getvalue()
        '\x00\x03\x00\x01\x00\x02\x00\x03'

        >>> f = StringIO.StringIO()
        >>> s._write(f, {'a':1, 'b':2}, OrderedDict((('a', 'int16'), ('b', 'int16'))))
        >>> f.getvalue()
        '\x01\x00\x01\x00\x02'

        It does some typechecking on the typedef parameter:
        >>> f = StringIO.StringIO()
        >>> s._write(f, 42, 0)
        Traceback (most recent call last):
        ...
        SerializationException: Unknown type: 0

        """

        if isinstance(typedef, dict):
            if obj is not None and not isinstance(obj, dict):
                raise SerializationException("Expected dict but got: %s" % type(obj))
            self._write_dict(output, obj, typedef)

        elif isinstance(typedef, list):
            if obj is not None and not isinstance(obj, list):
                raise SerializationException("Expected list but got: %s" % type(obj))
            self._write_list(output, obj, typedef)

        elif isinstance(typedef, str) or isinstance(typedef, unicode):
            getattr(self, "_write_" + typedef)(output, obj)

        else:
            raise SerializationException("Unknown type: %s" % typedef)


    def _write_boolean(self, output, b):
        r"""
        Internal routine that writes booleans:

        >>> s = JsonTypeSerializer('"string"')

        True is 1:
        >>> f = StringIO.StringIO()
        >>> s._write_boolean(f, True)
        >>> f.getvalue()
        '\x01'

        False is 0:
        >>> f = StringIO.StringIO()
        >>> s._write_boolean(f, False)
        >>> f.getvalue()
        '\x00'

        None is serialized to (int8)-1:
        >>> f = StringIO.StringIO()
        >>> s._write_boolean(f, None)
        >>> f.getvalue()
        '\xff'

        Only booleans are accepted:
        >>> f = StringIO.StringIO()
        >>> s._write_boolean(f, 42)
        Traceback (most recent call last):
        ...
        SerializationException: Expected bool but got: <type 'int'>
        """

        if b is None:
            output.write("\xFF")
        else:
            if not isinstance(b, bool):
                raise SerializationException("Expected bool but got: %s" % type(b))

            if b:
                output.write("\x01")
            else:
                output.write("\x00")

    def _write_numeric(self, output, n, typedef):
        r"""
        Internal routine that writes numeric data. The individual _write_* methods are synthesized as
        calls to this method.

        >>> s = JsonTypeSerializer('"string"')

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 42, 'int8')
        >>> f.getvalue()
        '*'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 42, 'int16')
        >>> f.getvalue()
        '\x00*'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 42, 'int32')
        >>> f.getvalue()
        '\x00\x00\x00*'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 42, 'int64')
        >>> f.getvalue()
        '\x00\x00\x00\x00\x00\x00\x00*'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 0.25, 'float32')
        >>> f.getvalue()
        '>\x80\x00\x00'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 0.25, 'float64')
        >>> f.getvalue()
        '?\xd0\x00\x00\x00\x00\x00\x00'

        None is serialized to the minimum value for each type:
        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, None, 'int8')
        >>> f.getvalue()
        '\x80'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, None, 'int16')
        >>> f.getvalue()
        '\x80\x00'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, None, 'int32')
        >>> f.getvalue()
        '\x80\x00\x00\x00'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, None, 'int64')
        >>> f.getvalue()
        '\x80\x00\x00\x00\x00\x00\x00\x00'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, None, 'float32')
        >>> f.getvalue()
        '\x00\x00\x00\x01'

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, None, 'float64')
        >>> f.getvalue()
        '\x00\x00\x00\x00\x00\x00\x00\x01'

        Basic typechecking is done:
        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 0.25, 'int16')
        Traceback (most recent call last):
        ...
        SerializationException: Invalid type: <type 'float'> for typedef: int16

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 42, 'float32')
        Traceback (most recent call last):
        ...
        SerializationException: Invalid type: <type 'int'> for typedef: float32

        Range checking is also done:
        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 500, 'int8')
        Traceback (most recent call last):
        ...
        SerializationException: Value 500 out of range for typedef: int8

        >>> f = StringIO.StringIO()
        >>> s._write_numeric(f, 1.0e-45, 'float32')
        Traceback (most recent call last):
        ...
        SerializationException: Value 1e-45 out of range for typedef: float32
        """

        if not isinstance(typedef, str) and not isinstance(typedef, unicode):
            raise SerializationException("Typedef must be a str, got: %s" % type(typedef))

        if typedef not in set(["int8", "int16", "int32", "int64", "float32", "float64"]):
            raise SerializationException("Invalid typedef: %s" % typedef)

        if n is None:
            n = MINS[typedef]
        else:
            if not any(isinstance(n, t) for t in TYPES[typedef]):
                raise SerializationException("Invalid type: %s for typedef: %s" % (type(n), typedef))

            if not RANGE_FNS[typedef](n, typedef):
                raise SerializationException("Value %s out of range for typedef: %s" % (n, typedef))

        output.write(struct.pack(FORMATS[typedef], n))

    def _write_date(self, output, d):
        r"""
        Internal routine that serializes dates.

        >>> s = JsonTypeSerializer('"string"')
        >>> d = datetime.datetime(2010, 11, 24, 17, 36, 11, 410413)

        >>> f = StringIO.StringIO()
        >>> s._write_date(f, d)
        >>> f.getvalue()
        '\x00\x00\x01,~\xf4\xf4\x92'

        None serializers to the smallest representable int64:
        >>> f = StringIO.StringIO()
        >>> s._write_date(f, None)
        >>> f.getvalue()
        '\x80\x00\x00\x00\x00\x00\x00\x00'

        Object passed in must be a date
        >>> f = StringIO.StringIO()
        >>> s._write_date(f, 42)
        Traceback (most recent call last):
        ...
        SerializationException: Expected datetime but got: <type 'int'>

        """

        if d is None:
            self._write_int64(output, None)
            return

        if not isinstance(d, datetime.datetime):
            raise SerializationException("Expected datetime but got: %s" % type(d))

        self._write_int64(output, _to_java_date(d))

    def _write_bytes(self, output, bs):
        r"""
        Internal routine that writes raw bytes.

        >>> s = JsonTypeSerializer('"string"')

        >>> f = StringIO.StringIO()
        >>> s._write_bytes(f, 'foo')
        >>> f.getvalue()
        '\x00\x03foo'

        None is serialized as -1 length byte string:
        >>> f = StringIO.StringIO()
        >>> s._write_bytes(f, None)
        >>> f.getvalue()
        '\xff\xff'

        The empty string is a 0 length byte string:
        >>> f = StringIO.StringIO()
        >>> s._write_bytes(f, '')
        >>> f.getvalue()
        '\x00\x00'
        """

        if bs is None:
            self._write_length(output, -1)
            return

        self._write_length(output, len(bs))
        output.write(bs)

    def _write_length(self, output, size):
        r"""
        Internal routine that writes sequence lengths in Voldemort's variable length format.

        >>> s = JsonTypeSerializer('"string"')

        Small lengths are encoded as int16s:
        >>> f = StringIO.StringIO()
        >>> s._write_length(f, 10)
        >>> f.getvalue()
        '\x00\n'

        Length of -1 (magic length of null sequences) is encoded as (int16)-1:
        >>> f = StringIO.StringIO()
        >>> s._write_length(f, -1)
        >>> f.getvalue()
        '\xff\xff'

        Longer lengths are encoded as int32s with the high two bits set:
        >>> f = StringIO.StringIO()
        >>> s._write_length(f, 1000000)
        >>> f.getvalue()
        '\xc0\x0fB@'

        Lengths between 0x3fff0000 and 0x3fffffff are supposed to be supported but will serialize
        to a byte string that won't get decoded properly:
        >>> f = StringIO.StringIO()
        >>> s._write_length(f, 0x3fff0001)
        >>> bytes = f.getvalue()
        >>> bytes
        '\xff\xff\x00\x01'

        >>> s._read_length(StringIO.StringIO(bytes))
        -1

        """

        if size < MAXES["int16"]:
            self._write_int16(output, size)
        elif size <= MAX_SEQ_LENGTH:
            self._write_int32(output, size | -0x40000000) # equivalent to 0xc0000000
        else:
            raise SerializationException("Length of %d exceeds maximum allowed: %d" % (size, MAX_SEQ_LENGTH))

    def _write_string(self, output, s):
        r"""
        Internal routine for serializing UTF-8 strings.

        >>> s = JsonTypeSerializer('"string"')

        >>> f = StringIO.StringIO()
        >>> s._write_string(f, 'foo')
        >>> f.getvalue()
        '\x00\x03foo'

        Unicode works, too:
        >>> f = StringIO.StringIO()
        >>> s._write_string(f, u'foo')
        >>> f.getvalue()
        '\x00\x03foo'

        Other types will cause an error:
        >>> f = StringIO.StringIO()
        >>> s._write_string(f, 42)
        Traceback (most recent call last):
        ...
        SerializationException: Expected string or unicode and got: <type 'int'>

        The empty string turns into a zero-length string:
        >>> f = StringIO.StringIO()
        >>> s._write_string(f, u'')
        >>> f.getvalue()
        '\x00\x00'

        None turns into a -1 length string:
        >>> f = StringIO.StringIO()
        >>> s._write_string(f, None)
        >>> f.getvalue()
        '\xff\xff'
        """

        if s is None:
            self._write_bytes(output, None)
        else:
            if not isinstance(s, str) and not isinstance(s, unicode):
                raise SerializationException("Expected string or unicode and got: %s" % type(s))
            self._write_bytes(output, s.encode("utf_8"))

    def _write_list(self, output, items, typedef):
        r"""
        Internal method for serializing lists.

        >>> s = JsonTypeSerializer('"string"')

        >>> f = StringIO.StringIO()
        >>> s._write_list(f, [1,2,3], ['int16'])
        >>> f.getvalue()
        '\x00\x03\x00\x01\x00\x02\x00\x03'

        Empty lists are encoded as length 0:
        >>> f = StringIO.StringIO()
        >>> s._write_list(f, [], ['int16'])
        >>> f.getvalue()
        '\x00\x00'

        None is encoded as length -1:
        >>> f = StringIO.StringIO()
        >>> s._write_list(f, None, ['int16'])
        >>> f.getvalue()
        '\xff\xff'

        Input must be a list:
        >>> f = StringIO.StringIO()
        >>> s._write_list(f, 1, ['int16'])
        Traceback (most recent call last):
        ...
        TypeError: object of type 'int' has no len()

        Typedef must be a singleton list:
        >>> f = StringIO.StringIO()
        >>> s._write_list(f, [1,2,3], [])
        Traceback (most recent call last):
        ...
        SerializationException: Type declaration of a list must be a singleton list.

        >>> f = StringIO.StringIO()
        >>> s._write_list(f, [1,2,3], ['int16', 'int16'])
        Traceback (most recent call last):
        ...
        SerializationException: Type declaration of a list must be a singleton list.

        >>> f = StringIO.StringIO()
        >>> s._write_list(f, [1,2,3], 'int16')
        Traceback (most recent call last):
        ...
        SerializationException: Type declaration of a list must be a singleton list.

        """

        if not isinstance(typedef, list) or len(typedef) != 1:
            raise SerializationException("Type declaration of a list must be a singleton list.")

        objtype = typedef[0]
        if items is None:
            self._write_length(output, -1)
        else:
            self._write_length(output, len(items))
            for item in items:
                self._write(output, item, objtype)

    def _write_dict(self, output, items, typedef):
        r"""
        Internal routine that serializes dicts.

        >>> s = JsonTypeSerializer('"string"')

        >>> f = StringIO.StringIO()
        >>> s._write_dict(f, {'a':1, 'b':2}, OrderedDict((('a','int16'), ('b','int32'))))
        >>> f.getvalue()
        '\x01\x00\x01\x00\x00\x00\x02'

        None will serialize as (int8)-1:
        >>> f = StringIO.StringIO()
        >>> s._write_dict(f, None, OrderedDict((('a','int16'), ('b','int32'))))
        >>> f.getvalue()
        '\xff'

        The passed in dict must have the same fields as the typedef:
        >>> f = StringIO.StringIO()
        >>> s._write_dict(f, {'a':1}, OrderedDict((('a','int16'), ('b','int32'))))
        Traceback (most recent call last):
        ...
        SerializationException: Size mismatch for dict: expected 2 but got 1.

        >>> f = StringIO.StringIO()
        >>> s._write_dict(f, {'a':1, 'b':2, 'c':3}, OrderedDict((('a','int16'), ('b','int32'))))
        Traceback (most recent call last):
        ...
        SerializationException: Size mismatch for dict: expected 2 but got 3.

        >>> f = StringIO.StringIO()
        >>> s._write_dict(f, {'b':2, 'c':3}, OrderedDict((('a','int16'), ('b','int32'))))
        Traceback (most recent call last):
        ...
        SerializationException: Missing key: 'a' required by type: {'a':'int16', 'b':'int32'}

        The object being serialized must be a dict:

        >>> f = StringIO.StringIO()
        >>> s._write_dict(f, [1,2], OrderedDict((('a','int16'), ('b','int32'))))
        Traceback (most recent call last):
        ...
        SerializationException: Object must be a dict but got: <type 'list'>

        The typedef must be an OrderedDict:
        >>> f = StringIO.StringIO()
        >>> s._write_dict(f, {'b':2, 'c':3}, ['int16'])
        Traceback (most recent call last):
        ...
        SerializationException: Typedef must be an OrderedDict but got: <type 'list'>

        We can't use plain dicts as the typedef, because ordering is significant and plain old dicts
        don't guarantee order:
        >>> f = StringIO.StringIO()
        >>> s._write_dict(f, {'b':2, 'c':3}, {'a':'int16', 'b':'int16'})
        Traceback (most recent call last):
        ...
        SerializationException: Typedef must be an OrderedDict but got: <type 'dict'>
        """

        if not isinstance(typedef, OrderedDict):
            raise SerializationException("Typedef must be an OrderedDict but got: %s" % type(typedef))

        if items is None:
            self._write_int8(output, -1)
            return

        if not isinstance(items, dict):
            raise SerializationException("Object must be a dict but got: %s" % type(items))

        if len(items) != len(typedef):
            raise SerializationException("Size mismatch for dict: expected %d but got %d." % (len(typedef), len(items)))

        self._write_int8(output, 1)
        for key, entrytype in typedef.iteritems():
            if key not in items:
                raise SerializationException("Missing key: '%s' required by type: %s" % (key, typedef))

            self._write(output, items[key], entrytype)


# Generate the various primitive read/write methods:

def _make_methods(typedef):
    """
    Creates specialized _read_* and _write_* methods for the numeric types.
    """

    def _writer(self, output, obj):
        self._write_numeric(output, obj, str(typedef))

    def _reader(self, input):
        return self._read_numeric(input, str(typedef))

    setattr(JsonTypeSerializer, "_write_%s" % typedef, _writer)
    setattr(JsonTypeSerializer, "_read_%s" % typedef, _reader)

for typedef in MAXES.iterkeys():
    _make_methods(typedef)






