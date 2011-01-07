from common import *

class UnimplementedSerializer(object):
    def __init__(self, typename):
        self.typename = typename

    def reads(self, s):
        raise SerializationException("Unimplemented serialization type: %s" % self.typename)

    def writes(self, s):
        raise SerializationException("Unimplemented serialization type: %s" % self.typename)

    @staticmethod
    def create_from_xml(node):
        typenodes = [child for child in node.childNodes
                     if child.nodeType == minidom.Node.ELEMENT_NODE and child.tagName == "type"]
        typename = ''.join(node.data for node in typenodes[0].childNodes if node.nodeType == minidom.Node.TEXT_NODE)
        return UnimplementedSerializer(typename)
