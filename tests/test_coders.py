from aiolmdb.coders import IdentityCoder
from aiolmdb.coders import StringCoder
from aiolmdb.coders import UInt16Coder, UInt32Coder, UInt64Coder
from aiolmdb.coders import JSONCoder, PickleCoder
import unittest

PICKLE_TEST_CASES = [
    ({}, b'\x80\x03}q\x00.'),
    ([], b'\x80\x03]q\x00.'),
    ({"key": "value"},
     b'\x80\x03}q\x00X\x03\x00\x00\x00keyq\x01X\x05\x00\x00\x00valueq\x02s.')
]

COMPRESSED_TEST_CASES = [
    ({}, b'x\xda\xab\xae\x05\x00\x01u\x00\xf9'),
    ([], b'x\xda\x8b\x8e\x05\x00\x01\x15\x00\xb9'),
    ({"key": "value"},
        b'x\xda\xabV\xcaN\xadT\xb2RP*K\xcc)MU\xaa\x05\x00+\xaf\x05A')
]

JSON_TEST_CASES = [
    ({}, b'{}'),
    ([], b'[]'),
    ({"key": "value"}, b'{"key": "value"}')
]


class CoderTests(unittest.TestCase):

    def test_identity_serialize(self):
        test_cases = [b'0000', b'0003', b'0020', b'0wdl', b'oqda', b'fdqz', ]
        for case in test_cases:
            with self.subTest(input=case):
                self.assertEqual(case, IdentityCoder().serialize(case))

    def test_identity_deserialize(self):
        test_cases = [b'0000', b'0003', b'0020', b'0wdl', b'oqda', b'fdqz', ]
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

    def test_string_serialize(self):
        test_cases = [
            ("{}", b'{}'),
            ("[]", b'[]'),
            ('{"key": "value"}', b'{"key": "value"}')
        ]
        for val, enc in test_cases:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(enc, StringCoder().serialize(val))

    def test_string_deserialize(self):
        test_cases = [
            ("{}", b'{}'),
            ("[]", b'[]'),
            ('{"key": "value"}', b'{"key": "value"}')
        ]
        for val, enc in test_cases:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(val, StringCoder().deserialize(enc))

    def test_json_serialize(self):
        for val, enc in JSON_TEST_CASES:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(enc, JSONCoder().serialize(val))

    def test_json_deserialize(self):
        for val, enc in JSON_TEST_CASES:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(val, JSONCoder().deserialize(enc))

    def test_compressed_deserialize(self):
        coder = JSONCoder().compressed(9)
        for val, enc in COMPRESSED_TEST_CASES:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(val, coder.deserialize(enc))

    def test_compressed_serialize(self):
        coder = JSONCoder().compressed(9)
        for val, enc in COMPRESSED_TEST_CASES:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(enc, coder.serialize(val))

    def test_pickle_serialize(self):
        for val, enc in PICKLE_TEST_CASES:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(enc, PickleCoder().serialize(val))

    def test_pickle_deserialize(self):
        for val, enc in PICKLE_TEST_CASES:
            with self.subTest(val=val, enc=enc):
                self.assertEqual(val, PickleCoder().deserialize(enc))


if __name__ == '__main__':
    unittest.main()
