import json
import struct
import pickle
import zlib
from abc import abstractmethod


class Coder():

    @abstractmethod
    def serialize(self, obj):
        pass

    @abstractmethod
    def deserialize(self, buf):
        pass

    def compressed(self, level=1):
        return ZlibCoder(self, level)


class IdentityCoder(Coder):

    def serialize(self, obj):
        return bytes(obj)

    def deserialize(self, buf):
        return None if buf is None else bytes(buf)


class StringCoder(Coder):

    def __init__(self, encoding="utf8"):
        self.encoding = encoding

    def serialize(self, obj):
        return obj.encode(self.encoding)

    def deserialize(self, buf):
        return None if buf is None else buf.decode(self.encoding)


def __create_int_coder(name, fmt):

    class IntCoder(Coder):

        def serialize(self, obj):
            return struct.pack(fmt, obj)

        def deserialize(self, buf):
            return None if buf is None else struct.unpack(fmt, buf)[0]

    IntCoder.__name__ = name
    return IntCoder


UInt16Coder = __create_int_coder("UInt16Coder", ">H")
UInt32Coder = __create_int_coder("UInt32Coder", ">I")
UInt64Coder = __create_int_coder("UInt64Coder", ">Q")


class PickleCoder(Coder):

    def serialize(self, obj):
        return pickle.dumps(obj)

    def deserialize(self, buf):
        return None if buf is None else pickle.loads(buf)


class JSONCoder(StringCoder):

    def serialize(self, obj):
        json_str = json.dumps(obj, ensure_ascii=False)
        return super(JSONCoder, self).serialize(json_str)

    def deserialize(self, buf):
        if buf is None:
            return None
        return json.loads(super(JSONCoder, self).deserialize(buf))


class ZlibCoder(Coder):

    def __init__(self, subcoder, level=1):
        self.subcoder = subcoder
        self.level = level

    def serialize(self, obj):
        buf = self.subcoder.serialize(obj)
        return zlib.compress(buf, self.level)

    def deserialize(self, buf):
        if buf is None:
            return None
        decomp_buf = zlib.decompress(buf)
        return self.subcoder.deserialize(decomp_buf)
