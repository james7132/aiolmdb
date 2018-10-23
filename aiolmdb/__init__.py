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
    Creates a new async lmdb enviroment. All arguments are passed to lmdb.open to
    create the enviroment.
    """
    lmdb_env = lmdb.open(*args, **kwargs)
    return AsyncEnviroment(env)


def version():
      return lmdb.version()


def __action(async_db, action, write):
    with env.begin(write=write, db=async_db.db_handle, buffers=True) as txn:
        return action(txn)


class AsyncEnviroment():
    """An asyncio wrapper around lmdb.Enviroment."""

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
        Closes the enviroment, invalidating any open iterators, cursors and
        transactions

        Repeat calls to close() have no effect.
        """
        self.env.close()

    def get_default_db(self):
        """Gets an AsyncDatabase wrapping the default database for the enviroment"""
        return self._default_db

    def open_db(self, name, *args, **kwargs):
        """
        Opens a child database with the provided name. args and kwargs are passed
        into the `AsyncDatabase` constructor.
        """
        return AsyncDatabase(self, self.env.open_db(name), *args, **kwargs)

    async def copy(self, path, compact=False):
        """|coro|
        Make a consistent copy of the environment in the given destination directory.

        `compact`:
            If ``True``, perform compaction while copying: omit free pages and
            sequentially renumber all pages in output. This option consumes
            more CPU and runs more slowly than the default, but may produce a
            smaller output database.

        Equivalent to `mdb_env_copy()
        <http://symas.com/mdb/doc/group__mdb.html#ga5d51d6130325f7353db0955dbedbc378>`_
        """
        def __copy_action():
          return self.env.copy(path, compact=compact)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, __copy_action)

    async def copyfd(self, fd, compact=False):
        """|coro|
        Copy a consistent version of the environment to file descriptor `fd`.

        `compact`:
            If ``True``, perform compaction while copying: omit free pages and
            sequentially renumber all pages in output. This option consumes
            more CPU and runs more slowly than the default, but may produce a
            smaller output database.

        Equivalent to `mdb_env_copyfd()
        <http://symas.com/mdb/doc/group__mdb.html#ga5d51d6130325f7353db0955dbedbc378>`_
        """
        def __copyfd_action():
          return self.env.copyfd(fd, compact=compact)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, __copyfd_action)

    async def sync(self, force=False):
        """|coro|
        Flush the data buffers to disk.

        Equivalent to `mdb_env_sync()
        <http://symas.com/mdb/doc/group__mdb.html#ga85e61f05aa68b520cc6c3b981dba5037>`_

        Data is always written to disk when :py:meth:`Transaction.commit` is
        called, but the operating system may keep it buffered. MDB always
        flushes the OS buffers upon commit as well, unless the environment was
        opened with `sync=False` or `metasync=False`.

        `force`:
            If ``True``, force a synchronous flush. Otherwise if the
            environment was opened with `sync=False` the flushes will be
            omitted, and with `map_async=True` they will be asynchronous.
        """
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

        `action`:
            A one argument function that takes a `lmdb.Transaction` that is executed
            synchronously from a thread. The transaction is disposed of and commited
            after the function exits. All buffers that have been read from database
            during the function's execution will become invalid, data must be copied
            to remain valid.

        `write`:
            If ``True``, starts a read-write transaction, otherwise starts a
            read-only transaction. Write transactions will block the thread they are
            executing if there are contending write transactions.
        """
        return await self.async_env._run_action(self, action, write=write)

    async def stat(self):
        """|coro|
        Return statistics like :py:meth:`Environment.stat`, except for a single
        DBI. `db` must be a database handle returned by :py:meth:`open_db`.
        """
        def __stat_action(txn):
            return txn.stat(self.db)
        return await self.run(__stat_action)

    async def get(self, key, default=None):
        """|coro|
        Fetch the first value matching `key`, returning `default` if `key`
        does not exist. A cursor must be used to fetch all values for a key in
        a `dupsort=True` database.

        Equivalent to `mdb_get()
        <http://symas.com/mdb/doc/group__mdb.html#ga8bf10cd91d3f3a83a34d04ce6b07992d>`_
        """
        def __get_action(txn):
            key_enc = self.key_coder.serialize(key)
            buf = txn.get(key_enc)
            return self.value_coder.deserialize(buf)
        return await self.run(__get_action) or default

    async def pop(self, key):
        """|coro|
        Use a temporary cursor to invoke :py:meth:`Cursor.pop` on a key.
        """
        def __pop_action(txn):
            key_enc = self.key_coder.serialize(key)
            buf = txn.pop(key_enc)
            return self.value_coder.deserialize(buf)
        return await self.run(__pop_action, write=True)

    async def replace(self, key, value):
        """|coro|
        Use a temporary cursor to invoke :py:meth:`Cursor.replace`.
        """
        def __replace_action(txn):
            key_enc = self.key_coder.serialize(key)
            value_enc = self.value_coder.serialize(value)
            buf = txn.replace(key_enc, value_enc)
            return value_coder.deserialize(buf)
        return await self.run(__replace_action, write=True)

    async def put(self, key, value, dupdata=True, overwrite=True):
        """|coro|
        Store a record, returning ``True`` if it was written, or ``False``
        to indicate the key was already present and `overwrite=False`. On
        success, the cursor is positioned on the new record.

        Equivalent to `mdb_put()
        <http://symas.com/mdb/doc/group__mdb.html#ga4fa8573d9236d54687c61827ebf8cac0>`_

        `key`:
            Key to store, corresponding to the provided `key_encoder` of the
            database.

        `value`:
            Value to store, corresponding to the provided `key_encoder` of the
            database.

        `dupdata`:
            If ``True`` and database was opened with `dupsort=True`, add
            pair as a duplicate if the given key already exists. Otherwise
            overwrite any existing matching key.

        `overwrite`:
            If ``False``, do not overwrite any existing matching key.
        """
        def __put_action(txn):
            key_enc = self.key_coder.serialize(key)
            value_enc = self.value_coder.serialize(value)
            return txn.put(key_enc, value_enc, dupdata=dupdata, overwrite=overwrite)
        return await self.run(__put_action, write=True)

    async def delete(self, key, value=None):
        """|coro|
        Delete a key from the database.

        Equivalent to `mdb_del()
        <http://symas.com/mdb/doc/group__mdb.html#gab8182f9360ea69ac0afd4a4eaab1ddb0>`_

        `key`:
            The key to delete.
        value:
            If the database was opened with dupsort=True and value is not
            the empty bytestring, then delete elements matching only this
            `(key, value)` pair, otherwise all values for key are deleted.

        Returns True if at least one key was deleted.
        """
        def __delete_action(txn):
            key_enc = self.key_coder.serialize(key)
            value_enc = b'' if value is None else self.value_coder.serialize(
                value)
            return txn.delete(key_enc, value_enc, self.db)
        return await self.run(__delete_action, write=True)

    async def get_multi(self, keys):
        """|coro|
        Gets multiple keys from the database. Returns a dict of {key, value}.

        `keys`:
        An iterable of keys to retrieve from the database.
        """
        def __get_multi_action(txn):
            keys_enc = ((key, self.key_coder.serialize(key)) for key in keys)
            return {key: self.value_coder.deserialize(txn.get(key))
                    for key, enc in keys_enc}
        return await self.run(__get_multi_action)

    async def put_multi(self, items):
        """|coro|
        Sets multiple (key, value) tuples in the database.

        `items`:
        An iterable of (key, value) tuples to set.
        """
        def __put_multi_action(txn):
            with txn.cursor(self.db) as csr:
                items_enc = [(self.key_coder.serialize(key),
                              self.value_coder.serialize(value))
                             for key, value in items]
                return csr.put_multi(items_enc)
        return await self.run(__put_multi_action, write=True)

    async def delete_multi(self, keys):
        """|coro|
        Deletes multiple keys from the database. Returns a dictionary of {key, bool}
        each stating which key was successfully deleted.

        `items`:
        An iterable of keys to delete.
        """
        def __delete_multi_action(txn):
            return {key: txn.delete(self.key_coder.serialize(key)) for key in keys}
        return await self.run(__delete_multi_action, write=True)

    async def drop(self, delete=True):
        """|coro|
        Drops the database from the enviroment.

        `delete`:
        If ``True``, also deletes all values in the database.
        """
        def __drop_action(txn):
            return txn.drop(self.db)
        return await self.run(__delete_multi_action, write=True)
