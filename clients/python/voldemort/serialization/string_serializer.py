class StringSerializer(object):
    def reads(self, s):
        return s

    def writes(self, obj):
        return obj

    @staticmethod
    def create_from_xml(node):
        return StringSerializer()
