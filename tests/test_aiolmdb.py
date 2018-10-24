import os
import unittest

import aiolmdb
import asyncio
import lmdb
import sys
from tests import testlib
import weakref


NO_READERS = u'(no active readers)\n'

try:
    PAGE_SIZE = os.sysconf(os.sysconf_names['SC_PAGE_SIZE'])
except (AttributeError, KeyError, OSError):
    PAGE_SIZE = 4096


class VersionTest(testlib.AiolmdbTestCase):

    def test_version(self):
        ver = aiolmdb.version()
        assert len(ver) == 3
        assert all(isinstance(i, int) for i in ver)
        assert all(i >= 0 for i in ver)


class OpenTest(testlib.AiolmdbTestCase):

    def test_bad_paths(self):
        self.assertRaises(Exception,
                          lambda: aiolmdb.open('/doesnt/exist/at/all'))
        self.assertRaises(Exception,
                          lambda: aiolmdb.open(self.create_file()))

    def test_ok_path(self):
        path, env = self.create_env()
        assert os.path.exists(path)
        assert os.path.exists(os.path.join(path, 'data.mdb'))
        assert os.path.exists(os.path.join(path, 'lock.mdb'))
        assert env.path() == path

    def test_bad_size(self):
        self.assertRaises(OverflowError,
                          lambda: self.create_env(map_size=-123))

    def test_tiny_size(self):
        _, env = self.create_env(map_size=10)

        @asyncio.coroutine
        def txn():
            yield from env.get_default_db().put(b'a', b'a')
        self.assertAsyncRaises(lmdb.MapFullError, txn)

    def test_subdir_false_junk(self):
        path = self.create_file()
        fp = open(path, 'wb')
        fp.write(b'A' * 8192)
        fp.close()
        self.assertRaises(lmdb.InvalidError,
                          lambda: aiolmdb.open(path, subdir=False))

    def test_subdir_false_ok(self):
        path = self.create_file(create=False)
        _, env = self.create_env(path, subdir=False)
        assert os.path.exists(path)
        assert os.path.isfile(path)
        assert os.path.isfile(path + '-lock')
        assert not env.flags()['subdir']

    def test_subdir_true_noexist_nocreate(self):
        path = self.create_dir(create=False)
        self.assertRaises(lmdb.Error,
                          lambda: self.create_env(path, subdir=True,
                                                  create=False))
        assert not os.path.exists(path)

    def test_subdir_true_noexist_create(self):
        path = self.create_dir(create=False)
        path_, env = self.create_env(path, subdir=True, create=True)
        assert path_ == path
        assert env.path() == path

    def test_subdir_true_exist_nocreate(self):
        path, env = self.create_env()
        assert aiolmdb.open(path, subdir=True, create=False).path() == path

    def test_subdir_true_exist_create(self):
        path, env = self.create_env()
        assert aiolmdb.open(path, subdir=True, create=True).path() == path

    @asyncio.coroutine
    def test_readonly_false(self):
        path, env = self.create_env(readonly=False)
        db = env.get_default_db()
        yield from db.put(b'a', b'')
        assert (yield from db.get(b'a')) == b''
        assert not env.flags()['readonly']

    def test_readonly_true_noexist(self):
        path = self.create_dir(create=False)
        # Open readonly missing store should fail.
        self.assertRaises(lmdb.Error,
                          lambda: aiolmdb.open(path, readonly=True,
                                               create=True))
        # And create=True should not have mkdir'd it.
        assert not os.path.exists(path)

    def test_readonly_true_exist(self):
        path, env = self.create_env()
        env2 = aiolmdb.open(path, readonly=True)
        assert env2.path() == path
        # Attempting a write txn should fail.

        @asyncio.coroutine
        def txn(txn):
            yield from env2.get_default_db().put(b'a', b'a')
        self.assertAsyncRaises(lmdb.ReadonlyError, txn)
        # Flag should be set.
        assert env2.flags()['readonly']

    def test_metasync(self):
        for flag in True, False:
            path, env = self.create_env(metasync=flag)
            assert env.flags()['metasync'] == flag

    def test_lock(self):
        for flag in True, False:
            path, env = self.create_env(lock=flag)
            lock_path = os.path.join(path, 'lock.mdb')
            assert env.flags()['lock'] == flag
            assert flag == os.path.exists(lock_path)

    def test_sync(self):
        for flag in True, False:
            path, env = self.create_env(sync=flag)
            assert env.flags()['sync'] == flag

    def test_map_async(self):
        for flag in True, False:
            path, env = self.create_env(map_async=flag)
            assert env.flags()['map_async'] == flag

    def test_mode_subdir_create(self):
        if sys.platform == 'win32':
            # Mode argument is ignored on Windows; see aiolmdb.h
            return

        oldmask = os.umask(0)
        try:
            for mode in 0o777, 0o755, 0o700:
                path = self.create_dir(create=False)
                aiolmdb.open(path, subdir=True, create=True, mode=mode)
                fmode = mode & ~0o111
                assert self.path_mode(path) == mode
                assert self.path_mode(path+'/data.mdb') == fmode
                assert self.path_mode(path+'/lock.mdb') == fmode
        finally:
            os.umask(oldmask)

    def test_mode_subdir_nocreate(self):
        if sys.platform == 'win32':
            # Mode argument is ignored on Windows; see aiolmdb.h
            return

        oldmask = os.umask(0)
        try:
            for mode in 0o777, 0o755, 0o700:
                path = self.create_dir()
                aiolmdb.open(path, subdir=True, create=False, mode=mode)
                fmode = mode & ~0o111
                assert self.path_mode(path+'/data.mdb') == fmode
                assert self.path_mode(path+'/lock.mdb') == fmode
        finally:
            os.umask(oldmask)

    def test_readahead(self):
        for flag in True, False:
            path, env = self.create_env(readahead=flag)
            assert env.flags()['readahead'] == flag

    def test_writemap(self):
        for flag in True, False:
            path, env = self.create_env(writemap=flag)
            assert env.flags()['writemap'] == flag

    def test_meminit(self):
        for flag in True, False:
            path, env = self.create_env(meminit=flag)
            assert env.flags()['meminit'] == flag

    def test_max_readers(self):
        self.assertRaises(lmdb.InvalidParameterError,
                          lambda: self.create_env(max_readers=0))
        for val in 123, 234:
            _, env = self.create_env(max_readers=val)
            assert env.info()['max_readers'] == val

    def test_max_dbs(self):
        self.assertRaises(OverflowError,
                          lambda: self.create_env(max_dbs=-1))
        for val in 0, 10, 20:
            _, env = self.create_env(max_dbs=val)
            [env.open_db(('db%d' % i).encode()) for i in range(val)]
            self.assertRaises(lmdb.DbsFullError,
                              lambda: env.open_db(b'toomany'))


class SetMapSizeTest(testlib.AiolmdbTestCase):

    def test_invalid(self):
        _, env = self.create_env()
        env.close()
        self.assertRaises(Exception,
                          lambda: env.set_mapsize(999999))

    def test_negative(self):
        _, env = self.create_env()
        self.assertRaises(OverflowError,
                          lambda: env.set_mapsize(-2015))

    def test_applied(self):
        _, env = self.create_env(map_size=PAGE_SIZE * 8)
        assert env.info()['map_size'] == PAGE_SIZE * 8

        env.set_mapsize(PAGE_SIZE * 16)
        assert env.info()['map_size'] == PAGE_SIZE * 16


class CloseTest(testlib.AiolmdbTestCase):

    @asyncio.coroutine
    def test_close(self):
        _, env = self.create_env()
        # Attempting things should be ok.
        yield from env.get_default_db().put(b'a', b'')
        # txn.put(b'a', b'')
        # cursor = txn.cursor()
        # list(cursor)
        # cursor.first()
        # it = iter(cursor)

        env.close()
        # Repeated calls are ignored:
        env.close()
        # Attempting to use invalid objects should crash.
        # self.assertRaises(Exception, lambda: txn.cursor())
        # self.assertRaises(Exception, lambda: txn.commit())
        # self.assertRaises(Exception, lambda: cursor.first())
        # self.assertRaises(Exception, lambda: list(it))
        # Abort should be OK though.
        # txn.abort()
        # Attempting to start new txn should crash.

        @asyncio.coroutine
        def txn():
            yield from env.get_default_db().get(b'a')
        self.assertAsyncRaises(Exception, txn())


class ContextManagerTest(testlib.AiolmdbTestCase):

    @asyncio.coroutine
    def test_ok(self):
        path, env = self.create_env()
        with env as env_:
            assert env_ is env
            yield from env.get_default_db().get(b'foo')

        @asyncio.coroutine
        def txn():
            yield from env.get_default_db().get(b'foo')
        self.assertAsyncRaises(Exception, txn())

    @asyncio.coroutine
    def test_crash(self):
        path, env = self.create_env()
        try:
            with env as env_:
                assert env_ is env
                yield from env.get_default_db().get(123)
        except Exception:
            pass

        @asyncio.coroutine
        def txn():
            yield from env.get_default_db().get(b'foo')
        self.assertAsyncRaises(Exception, txn())


class InfoMethodsTest(testlib.AiolmdbTestCase):

    def test_path(self):
        path, env = self.create_env()
        assert path == env.path()
        assert isinstance(env.path(), str)

        env.close()
        self.assertRaises(Exception,
                          lambda: env.path())

    @asyncio.coroutine
    def test_stat(self):
        _, env = self.create_env()
        stat = env.stat()
        for k in 'psize', 'depth', 'branch_pages', 'overflow_pages',\
                 'entries':
            assert isinstance(stat[k], int), k
            assert stat[k] >= 0

        assert stat['entries'] == 0
        db = env.get_default_db()
        yield from db.put(b'a', b'b')
        stat = env.stat()
        assert stat['entries'] == 1

        env.close()
        self.assertRaises(Exception,
                          lambda: env.stat())

    @asyncio.coroutine
    def test_info(self):
        _, env = self.create_env()
        info = env.info()
        for k in 'map_addr', 'map_size', 'last_pgno', 'last_txnid', \
                 'max_readers', 'num_readers':
            assert isinstance(info[k], int), k
            assert info[k] >= 0
        assert info['last_txnid'] == 0
        db = env.get_default_db()
        yield from db.put(b'a', b'b')
        info = env.info()
        assert info['last_txnid'] == 1

        env.close()
        self.assertRaises(Exception,
                          lambda: env.info())

    def test_flags(self):
        _, env = self.create_env()
        info = env.flags()
        for k in 'subdir', 'readonly', 'metasync', 'sync', 'map_async',\
                 'readahead', 'writemap':
            assert isinstance(info[k], bool)

        env.close()
        self.assertRaises(Exception,
                          lambda: env.flags())

    def test_max_key_size(self):
        _, env = self.create_env()
        mks = env.max_key_size()
        assert isinstance(mks, int)
        assert mks > 0

        env.close()
        self.assertRaises(Exception,
                          lambda: env.max_key_size())

    def test_max_readers(self):
        _, env = self.create_env()
        mr = env.max_readers()
        assert isinstance(mr, int)
        assert mr > 0 and mr == env.info()['max_readers']

        env.close()
        self.assertRaises(Exception,
                          lambda: env.max_readers())


class OtherMethodsTest(testlib.AiolmdbTestCase):

    @asyncio.coroutine
    def test_copy(self):
        _, env = self.create_env()
        yield from env.get_default_db().put(b'a', b'b')

        dest_dir = self.create_dir()
        yield from env.copy(dest_dir)
        assert os.path.exists(dest_dir + '/data.mdb')

        cenv = aiolmdb.open(dest_dir)
        self.assertEqual((yield from cenv.get_default_db().get(b'a')), b'b')

        env.close()

        @asyncio.coroutine
        def txn():
            yield from env.copy(self.create_dir())
        self.assertAsyncRaises(Exception, txn())

    @asyncio.coroutine
    def test_copy_compact(self):
        _, env = self.create_env()
        yield from env.get_default_db().put(b'a', b'b')

        dest_dir = self.create_dir()
        yield from env.copy(dest_dir, compact=True)
        assert os.path.exists(dest_dir + '/data.mdb')

        cenv = aiolmdb.open(dest_dir)
        self.assertEqual((yield from cenv.get_default_db().get(b'a')), b'b')

        env.close()

        @asyncio.coroutine
        def txn():
            yield from env.copy(self.create_dir())
        self.assertAsyncRaises(Exception, txn)

    @asyncio.coroutine
    def test_copyfd(self):
        path, env = self.create_env()
        yield from env.get_default_db().put(b'a', b'b')

        dst_path = self.create_file(create=False)
        fp = open(dst_path, 'wb')
        yield from env.copyfd(fp.fileno())

        dstenv = aiolmdb.open(dst_path, subdir=False)
        assert (yield from dstenv.get_default_db().get(b'a')) == b'b'

        env.close()

        @asyncio.coroutine
        def txn():
            yield from env.copyfd(fp.fileno())
        self.assertAsyncRaises(Exception, txn())
        fp.close()

    @asyncio.coroutine
    def test_copyfd_compact(self):
        path, env = self.create_env()
        yield from env.get_default_db().put(b'a', b'b')

        dst_path = self.create_file(create=False)
        fp = open(dst_path, 'wb')
        yield from env.copyfd(fp.fileno(), compact=True)

        dstenv = aiolmdb.open(dst_path, subdir=False)
        assert (yield from dstenv.get_default_db().get(b'a')) == b'b'

        env.close()

        @asyncio.coroutine
        def txn():
            yield from env.copyfd(fp.fileno())
        self.assertAsyncRaises(Exception, txn())
        fp.close()

    @asyncio.coroutine
    def test_sync(self):
        _, env = self.create_env()
        yield from env.sync(False)
        yield from env.sync(True)
        env.close()
        self.assertAsyncRaises(Exception, env.sync(False))

    @staticmethod
    def _test_reader_check_child(path):
        """Function to run in child process since we can't use fork() on
        win32."""
        env = aiolmdb.open(path, max_spare_txns=0)
        env.begin()
        os._exit(0)


class OpenDbTest(testlib.AiolmdbTestCase):

    def test_unicode(self):
        _, env = self.create_env()
        self.assertIsNotNone(env.open_db(b'myindex'))
        self.assertRaises(TypeError,
                          lambda: env.open_db(u'myindex'))

    def test_sub_notxn(self):
        _, env = self.create_env()
        self.assertEqual(env.info()['last_txnid'], 0)
        env.open_db(b'subdb1')
        self.assertEqual(env.info()['last_txnid'], 1)
        env.open_db(b'subdb2')
        self.assertEqual(env.info()['last_txnid'], 2)

        env.close()
        self.assertRaises(Exception,
                          lambda: env.open_db('subdb3'))

    def test_reopen(self):
        path, env = self.create_env()
        env.open_db(b'subdb1')
        env.close()
        env = aiolmdb.open(path, max_dbs=10)
        env.open_db(b'subdb1')

    FLAG_SETS = [
        (flag, val)
        for flag in (
            'reverse_key', 'dupsort', 'integerkey', 'integerdup', 'dupfixed'
        )
        for val in (True, False)
    ]

    def test_readonly_env_main(self):
        path, env = self.create_env()
        env.close()

        env = aiolmdb.open(path, readonly=True)
        env.open_db(None)

    def test_readonly_env_sub_noexist(self):
        # https://github.com/dw/py-aiolmdb/issues/109
        path, env = self.create_env()
        env.close()

        env = aiolmdb.open(path, max_dbs=10, readonly=True)
        self.assertRaises(lmdb.NotFoundError,
                          lambda: env.open_db(b'node_schedules', create=False))

    def test_readonly_env_sub_eperm(self):
        # https://github.com/dw/py-aiolmdb/issues/109
        path, env = self.create_env()
        env.close()

        env = aiolmdb.open(path, max_dbs=10, readonly=True)
        self.assertRaises(lmdb.ReadonlyError,
                          lambda: env.open_db(b'node_schedules', create=True))

    def test_readonly_env_sub(self):
        # https://github.com/dw/py-aiolmdb/issues/109
        path, env = self.create_env()
        self.assertIsNotNone(env.open_db(b'node_schedules'))
        env.close()

        env = aiolmdb.open(path, max_dbs=10, readonly=True)
        db = env.open_db(b'node_schedules', create=False)
        self.assertIsNotNone(db)


def reader_count(env): return env.readers().count('\n') - 1


class LeakTest(testlib.AiolmdbTestCase):

    def test_open_unref_does_not_leak(self):
        temp_dir = self.create_dir()
        env = aiolmdb.open(temp_dir)
        ref = weakref.ref(env)
        env = None
        testlib.debug_collect()
        self.assertIsNone(ref())

    def test_open_close_does_not_leak(self):
        temp_dir = self.create_dir()
        env = aiolmdb.open(temp_dir)
        env.close()
        ref = weakref.ref(env)
        env = None
        testlib.debug_collect()
        self.assertIsNone(ref())

    def test_weakref_callback_invoked_once(self):
        temp_dir = self.create_dir()
        env = aiolmdb.open(temp_dir)
        env.close()
        count = [0]

        def callback(ref):
            count[0] += 1
        ref = weakref.ref(env, callback)
        env = None
        testlib.debug_collect()
        self.assertIsNone(ref())
        self.assertEqual(count[0], 1)


if __name__ == '__main__':
    unittest.main()
