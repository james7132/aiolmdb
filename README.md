# aiolmdb [![Travis](https://travis-ci.org/james7132/aiolmdb.svg?branch=master)](https://travis-ci.org/james7132/aiolmdb)

An asyncio wrapper around LMDB.

aiolmdb is alpha quality software, expect the API or architecture to change
signifigantly over time.

## Usage

**Opening a aiolmdb enviroement**

```python
import aiolmdb

# Open a aiolmdb enviroment
#
# Takes the same arguments that lmdb.open does.
enviroment = aiolmdb.open("/tmp/path/to/enviorment", ...)
```

**Opening a aiolmdb database**

Unlike pylmdb, aiolmdb does not return a database handle on `open_db`, but
rather a full Python object.

```python
# Open a database
records = enviroment.open_db("records")

# Get the default ("" named database) within an enviroment
default = enviroment.get_default_database()
```

**Querying an aiolmdb database**

All queries against databases return coroutines and are run asynchronously.

```python
# Get a value(s) from the database
result = await db.get(b'key')              # Normal fetch, returned b'value'
result = await.db.get(b'key', default=b'') # Defaults to b'' if no key is found
result = await db.get_multi([b'0', b'1'])  # Gets multiple keys at once

# Write a value into the database
await db.put(b'key', b'value')
await db.put_multi([(b'k1', b'v1'), (b'k2', b'v2')]) # Puts multiple key-values
at once, atomically.

# Delete a key from the database
await db.delete(b'key')
await db.delete_multi([b'k1', b'k2', b'k3'])

# Drop the database
await db.drop()

# Run any arbitrary transactions
def transaction_action(txn):
  return txn.id()
await db.run(transaction_action)
```

**Using coders**

Applications do not operate directly on bytearrays, and require converting
runtime objects to and from serialized bytearrays. To avoid spending additional
time on the main loop running this conversion code, aiolmdb supports adding 
database level coders to run this serialization/deserialization logic in the 
executor instead of in the main loop. By default, every aiolmdb database uses 
the `IdentityCoder` which supports directly writing bytes like objects. Other
coders can be used for both the key and value to change the types of objects
accepted by the API.

```python
# Opening a database with specific coders
db = env.open_db("records", key_coder=UInt16Coder(), value_coder=JSONCoder())
await db.put(65535, {"key": "value"})   # Takes the approriate maching keys
await db.get(65535)                     # Returns {"key": "value"}

# Alter the coder for an existing database, useful for altering the enviroment
# default database.
db.key_coder = StringCoder()
db.value_coder = JSONCoder()

# Supported Coders
IdentityCoder()   # Raw bytes coder
StringCoder()     # String coder
UInt16Coder()     # 16-bit unsigned integer coder
UInt32Coder()     # 32-bit unsigned integer coder
UInt64Coder()     # 64-bit unsigned integer coder
JSONCoder()       # JSON coder, works with any JSON serializable object
PicleCoder()      # Pickle coder, works with any picklable object compression 

# Create a new JSONCoder, gzipped with compression level 9
# Runs the encoded JSON through zlib before writing to database, and
decompresses
zlib_json_coder = JSONCoder().compressed(level=9)
compressed_db = env.open_db("records", value_coder=zlib_json_coder)

# Write your own custom coder
from aiolmdb.coders import Coder

class CustomCoder(Coder):

  def serialize(self, obj):
    # Custom serialization logic
    #
    # These objects need to have locally immutable state: the objects must not
    # change how it represents its state for the duration of all concurrent
    # transactions dealing with the object.
    #
    # must return a bytes-like object
    return buffer

  def deserialize(self, buffer):
    # Custom deserialization logic
    #
    # aiolmdb uses LMDB transactions with `buffers=True`. this returns a
    # direct reference to the memory region. This buffer must NOT be modified in
    # any way. The lifetime of the buffer is also only valid during the scope of
    # the transaction that fetched it. To use the buffer outside of the context
    # of the serializer, it must be copied, and references to the buffer must
    # not be used elsewhere.
    #
    # Returns the deserialized object
    return deserialized_object
```

## Caveats and Gotchas

 * Write transactions (put, delete, pop, replace) still block while executed in
   the executor. Thus running multiple simultaneous write transactions will
   block all other transactions until they complete, one-by-one. Long running
   write transactions are strongly discouraged.
 * Due to design limitations, atomic transactions across multiple databases is
   currently not easy to do, nor is the code very pythonic.

## TODOs

 * Support cursors and range queries
