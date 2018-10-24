import aiolmdb
import asynctest
import gc
import os
import tempfile
import traceback
import shutil
import stat
import sys


class AiolmdbTestCase(asynctest.TestCase):

    def setUp(self):
        self.cleanups = []

    def tearDown(self):
        for cleanup in self.cleanups:
            try:
                cleanup()
            except Exception:
                traceback.print_exc()

    def create_dir(self, create=True):
        env_dir = tempfile.TemporaryDirectory()
        self.cleanups.append(lambda: env_dir.cleanup())

        path = tempfile.mkdtemp(prefix='lmdb_test')
        assert path is not None, 'tempfile.mkdtemp failed'
        if not create:
            os.rmdir(path)
        self.cleanups.append(lambda: shutil.rmtree(path, ignore_errors=True))
        if hasattr(path, 'decode'):
            path = path.decode(sys.getfilesystemencoding())
        return path

    def create_env(self, path=None, max_dbs=10, **kwargs):
        if not path:
            path = self.create_dir()
        env = aiolmdb.open(path, max_dbs=max_dbs, **kwargs)
        self.cleanups.append(env.close)
        return path, env

    def create_file(self, create=True):
        fd, path = tempfile.mkstemp(prefix='lmdb_test')
        assert path is not None, 'tempfile.mkstemp failed'
        os.close(fd)
        if not create:
            os.unlink(path)
        self.cleanups.append(lambda: os.path.exists(path) and os.unlink(path))
        pathlock = path + '-lock'
        self.cleanups.append(lambda: os.path.exists(
            pathlock) and os.unlink(pathlock))
        if hasattr(path, 'decode'):
            path = path.decode(sys.getfilesystemencoding())
        return path

    def path_mode(self, path):
        return stat.S_IMODE(os.stat(path).st_mode)


def debug_collect():
    if hasattr(gc, 'set_debug') and hasattr(gc, 'get_debug'):
        old = gc.get_debug()
        gc.set_debug(gc.DEBUG_LEAK)
        gc.collect()
        gc.set_debug(old)
    else:
        for x in range(10):
            # PyPy doesn't collect objects with __del__ on first attempt.
            gc.collect()
