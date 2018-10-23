import asyncio
import lmdb
import multiprocessing
import collections
from .coders import IdentityCoder
from concurrent.futures import ThreadPoolExecutor

__WRAPPED_ATTRS__ = [
    'info',
    'flags',
    'stat',
    'max_key_size',
    'max_readers',
    'path',
    'reader_check'
    'set_mapsize',
]

def open(*args, **kwargs):
  """
  Creates a new async lmdb enviroment.
  """
  lmdb_env = lmdb.open(*args, **kwargs)
  return AsyncEnviroment(env)

def version():
  return lmdb.version()

def __action(async_db, action, write):
  with env.begin(write=write, db=async_db.db_handle, buffers=True) as txn:
    return action(txn)

class AsyncEnviroment():

  def __init__(self, env, executor=None):
    worker_count = multiprocessing.cpu_count()

    self.env = env
    self.executor = executor or ThreadPoolExecutor(max_workers=worker_count)
    self._default_db = AsyncDatabase(self.env, None)

    for attr in __WRAPPED_ATTRS__:
        setattr(self, attr, getattr(self.env, attr))

  async def _run_action(self, async_db, action, write=False):
    args = (async_db, action,  write)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(self.executor, __action, args)

  def close(self):
    """
    Closes the enviroment, invalidating any open iterators, cursors and transactions.
    Repeat calls to close() have no effect.
    """
    self.env.close()
    self.executor.shutdown()

  def get_default_db(self):
    return self._default_db

  def open_db(self, name, *args, **kwargs):
    return AsyncDatabase(self, self.env.open_db(name), *args, **kwargs)

  async def copy(self, path, compact=False):
    def __copy_action():
      return self.env.copy(path, compact=compact)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(self.executor, __copy_action)

  async def copyfd(self, fd, compact=False):
    def __copyfd_action():
      return self.env.copyfd(fd, compact=compact)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(self.executor, __copyfd_action)

  async def sync(self, force=False):
    def __sync_action():
      return self.env.sync(force=force)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(self.executor, __sync_action)

class AsyncDatabase():

  def __init__(self, async_env, db_handle, key_coder=None, value_coder=None):
    self.async_env = async_env
    self.db_handle = db_handle
    self.key_coder = key_coder or IdentityCoder
    self.value_coder = value_coder or IdentityCoder

  async def run(self, action, write=False):
    """
    Runs an asynchronous operation with the database.
    """
    return await self.async_env._run_action(self, action, write=write)

  async def stat(self):
    def __stat_action(txn):
      return txn.stat(self.db)
    return await self.run(__stat_action)

  async def get(self, key):
    def __get_action(txn):
      key_enc = self.key_coder.serialize(key)
      buf = txn.get(key_enc)
      return self.value_coder.deserialize(buf)
    return await self.run(__get_action)

  async def pop(self, key):
    def __pop_action(txn):
      key_enc = self.key_coder.serialize(key)
      buf = txn.pop(key_enc)
      return self.value_coder.deserialize(buf)
    return await self.run(__pop_action, write=True)

  async def replace(self, key, value):
    def __replace_action(txn):
      key_enc = self.key_coder.serialize(key)
      value_enc = self.value_coder.serialize(value)
      buf = txn.replace(key_enc, value_enc)
      return value_coder.deserialize(buf)
    return await self.run(__replace_action, write=True)

  async def put(self, key, value):
    def __put_action(txn):
      key_enc = self.key_coder.serialize(key)
      value_enc = self.value_coder.serialize(value)
      return txn.put(key_enc, value_enc)
    return await self.run(__put_action, write=True)

  async def delete(self, key, value=None):
    """
    Deletes a key from the database.

    Equivalent to mdb_del()

      key: the key to delete

    Returns True if at least one key was deleted.
    """
    def __delete_action(txn):
      key_enc = self.key_coder.serialize(key)
      value_enc = b'' if value is None else self.value_coder.serialize(value)
      return txn.delete(key_enc, value_enc, self.db)
    return await self.run(__delete_action, write=True)

  async def get_multi(self, keys):
    def __get_multi_action(txn):
      keys_enc = (self.key_coder.serialize(key) in keys)
      return { key: self.value_coder.deserialize(txn.get(key)) for key in keys_enc }
    return await self.run(__get_multi_action)

  async def put_multi(self, items):
    def __put_multi_action(txn):
      with txn.cursor(self.db) as csr:

        return csr.put_multi(items)
    return await self.run(__put_multi_action, write=True)

  async def delete_multi(self, keys):
    def __delete_multi_action(txn):
      return { key: txn.delete(self.key_coder.serialize(key) for key in keys }
    return await self.run(__delete_multi_action, write=True)

  async def drop(self, delete=True):
    def __drop_action(txn):
      return txn.drop(self.db)
    return await self.run(__delete_multi_action, write=True)

  # TODO(james7132): Add prefix/iteration methods
