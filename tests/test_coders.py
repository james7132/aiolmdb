from aiolmdb.coders import *
import unittest

class CoderTests(unittest.TestCase):

    def test_identity_serialize(self):
        test_cases = [ b'0000', b'0003', b'0020', b'0wdl', b'oqda', b'fdqz', ]
        for case in test_cases:
            with self.subTest(input=case):
                self.assertEqual(case, IdentityCoder().serialize(case))

    def test_identity_deserialize(self):
        test_cases = [ b'0000', b'0003', b'0020', b'0wdl', b'oqda', b'fdqz', ]
        for case in test_cases:
            with self.subTest(input=case):
                self.assertEqual(case, IdentityCoder().deserialize(case))

    def test_uint16_serialize(self):
        test_cases = {
            0: b'\00\00',
            100: b'\x00d',
            255: b'\00\xff',
            65535: b'\xff\xff'
        }
        for val, enc in test_cases.items():
            with self.subTest(val=val, enc=enc):
                self.assertEqual(enc, UInt16Coder().serialize(val))

    def test_uint16_deserialize(self):
        test_cases = {
            0: b'\00\00',
            100: b'\x00d',
            255: b'\00\xff',
            65535: b'\xff\xff'
        }
        for val, enc in test_cases.items():
            with self.subTest(val=val, enc=enc):
                self.assertEqual(val, UInt16Coder().deserialize(enc))

    def test_uint32_serialize(self):
        test_cases = {
            0: b'\00\00\00\00',
            100: b'\00\00\x00d',
            255: b'\00\00\00\xff',
            65535: b'\00\00\xff\xff',
            2 ** 32 - 1: b'\xff\xff\xff\xff'
        }
        for val, enc in test_cases.items():
            with self.subTest(val=val, enc=enc):
                self.assertEqual(enc, UInt32Coder().serialize(val))

    def test_uint32_deserialize(self):
        test_cases = {
            0: b'\00\00\00\00',
            100: b'\00\00\x00d',
            255: b'\00\00\00\xff',
            65535: b'\00\00\xff\xff',
            2 ** 32 - 1: b'\xff\xff\xff\xff'
        }
        for val, enc in test_cases.items():
            with self.subTest(val=val, enc=enc):
                self.assertEqual(val, UInt32Coder().deserialize(enc))

    def test_uint64_serialize(self):
        test_cases = {
            0: b'\00\00\00\00\00\00\00\00',
            100: b'\00\00\00\00\00\00\x00d',
            255: b'\00\00\00\00\00\00\00\xff',
            65535: b'\00\00\00\00\00\00\xff\xff',
            2 ** 32 - 1: b'\00\00\00\00\xff\xff\xff\xff',
            2 ** 64 - 1: b'\xff\xff\xff\xff\xff\xff\xff\xff'
        }
        for val, enc in test_cases.items():
            with self.subTest(val=val, enc=enc):
                self.assertEqual(enc, UInt64Coder().serialize(val))

    def test_uint64_deserialize(self):
        test_cases = {
            0: b'\00\00\00\00\00\00\00\00',
            100: b'\00\00\00\00\00\00\x00d',
            255: b'\00\00\00\00\00\00\00\xff',
            65535: b'\00\00\00\00\00\00\xff\xff',
            2 ** 32 - 1: b'\00\00\00\00\xff\xff\xff\xff',
            2 ** 64 - 1: b'\xff\xff\xff\xff\xff\xff\xff\xff'
        }
        for val, enc in test_cases.items():
            with self.subTest(val=val, enc=enc):
                self.assertEqual(val, UInt64Coder().deserialize(enc))

    def test_json_serialize(self):
        test_cases = [
            ({}, b'{}'),
            ([], b'[]'),
            ({"key":"value"}, b'{"key": "value"}')
        ]
        for val, enc in test_cases:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(enc, JSONCoder().serialize(val))

    def test_json_deserialize(self):
        test_cases = [
            ({}, b'{}'),
            ([], b'[]'),
            ({"key":"value"}, b'{"key": "value"}')
        ]
        for val, enc in test_cases:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(val, JSONCoder().deserialize(enc))

    def test_pickle_serialize(self):
        test_cases = [
            ({}, b'\x80\x03}q\x00.'),
            ([], b'\x80\x03]q\x00.'),
            ({"key":"value"},
                b'\x80\x03}q\x00X\x03\x00\x00\x00keyq\x01X\x05\x00\x00\x00valueq\x02s.')
        ]
        for val, enc in test_cases:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(enc, PickleCoder().serialize(val))

    def test_json_deserialize(self):
        test_cases = [
            ({}, b'\x80\x03}q\x00.'),
            ([], b'\x80\x03]q\x00.'),
            ({"key":"value"},
                b'\x80\x03}q\x00X\x03\x00\x00\x00keyq\x01X\x05\x00\x00\x00valueq\x02s.')
        ]
        for val, enc in test_cases:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(val, PickleCoder().deserialize(enc))

if __name__ == '__main__':
    unittest.main()
