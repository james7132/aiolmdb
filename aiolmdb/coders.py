import json
import struct
import pickle
from abc import abstractmethod


class Coder():

  @abstractmethod
  def serialize(self, obj):
    pass

  @abstractmethod
  def deserialize(self, buffer):
    pass


class IdentityCoder(Coder):

  def serialize(self, obj):
    return bytes(obj)

  def deserialize(self, buffer):
    if buffer is None:
      return None
    return bytes(buffer)


def __create_int_coder(name, fmt):

  class IntCoder(Coder):

    def serialize(self, obj):
      return struct.pack(fmt, obj)

    def deserialize(self, buffer):
      if buffer is None:
        return None
      return struct.unpack(fmt, buffer)[0]

  IntCoder.__name__ = name
  return IntCoder


UInt16Coder = __create_int_coder("UInt16Coder", ">H")
UInt32Coder = __create_int_coder("UInt32Coder", ">I")
UInt64Coder = __create_int_coder("UInt64Coder", ">Q")


class PickleCoder(Coder):

  def serialize(self, obj):
    return pickle.dumps(obj)

  def deserialize(self, buffer):
    if buffer is None:
      return None
    return pickle.loads(buffer)


class JSONCoder(Coder):

  def __init__(self, encoding="utf8"):
    self.encoding = encoding

  def serialize(self, obj):
    return json.dumps(obj, ensure_ascii=False).encode(self.encoding)

  def deserialize(self, buffer):
    if buffer is None:
      return None
    return json.loads(buffer)
