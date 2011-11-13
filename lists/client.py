import pycassa, re
from entities             import UUID, Thread, Message
from pycassa.pool         import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from datetime             import datetime

class ThreadClient:
  def __init__(self, pool):
    self.th_fam = ColumnFamily(pool, 'threads') 

  # Public: Get a Thread.
  #
  # key - The String Thread key.
  #
  # Returns an entities.Thread.
  def get(self, key):
    try:
      return self.load(key, self.th_fam.get(key))
    except pycassa.cassandra.c10.ttypes.NotFoundException:
      pass

  # Public: Builds a new Thread object.  Args are passed to Thread().
  #
  # Returns an entities.Thread.
  def build(self, *args, **kwargs):
    return Thread(*args, **kwargs)

  # Public: Stores the Thread in Cassandra.
  #
  # thread - The entities.Thread to save.
  #
  # Returns nothing.
  def save(self, thread):
    self.th_fam.insert(thread.key, {
      'title':thread.title
    })

  # Builds a new Thread object from a Cassandra result.
  #
  # key    - The UUID key.
  # values - A Dict of Message attributes.
  #          title      - The String title.
  #          thread_key - The String Thread key.
  #          created_at - The DateTime creation timestamp.
  #          updated_at - The DateTime modification timestamp.
  #
  # Returns an entities.Thread.
  def load(self, key, values):
    return self.build(key, **values)

class MessageClient:
  def __init__(self, pool):
    self.th_msgs_fam = ColumnFamily(pool, 'thread_messages')
    self.msgs_fam    = ColumnFamily(pool, 'messages')

  # Public: Gets a range of Messages in a Thread.
  #
  # thread - a lists.Thread instance.
  #
  # Returns an Array of lists.Message instances.
  def list(self, thread):
    thread = Thread(thread)
    keys   = self.get_unique_msg_keys(thread)
    msgs   = []
    rows   = self.msgs_fam.multiget(keys)
    for key in rows:
      id     = UUID(key)
      values = rows[key]
      msgs.append(self.load(id, values))
    
    return msgs

  # Public: Gets a single Message.
  #
  # key - String Message UUID.
  #
  # Returns an entities.Message.
  def get(self, key):
    id     = UUID(key)
    values = self.msgs_fam.get(id.bytes)
    return self.load(id, values)

  # Public: Builds a new Message object.  Args are passed to Message().
  #
  # Returns an entities.Message.
  def build(self, *args, **kwargs):
    return Message(*args, **kwargs)

  # Public: Stores the Message in Cassandra and updates any indexes.
  #
  # msg - The entities.Message to save.
  #
  # Returns nothing.
  def save(self, msg):
    old_updated = None
    now = datetime.utcnow()
    if msg.key:
      old_updated = msg.updated_at
    else:
      msg.created_at = now
      msg.key = UUID()

    msg.updated_at = now
    columns        = {
      "thread_key": msg.thread.key, "title": msg.title,
      "created_at": msg.created_at, "updated_at": msg.updated_at,
    }
    self.msgs_fam.insert(msg.key.bytes, columns)
    self.update_msg_index(msg, old_updated)

  # Builds a new Thread object from a Cassandra result.
  #
  # key    - The UUID key.
  # values - A Dict of Message attributes.
  #          title      - The String title.
  #          thread_key - The String Thread key.
  #          created_at - The DateTime creation timestamp.
  #          updated_at - The DateTime modification timestamp.
  #
  # Returns an entities.Thread.
  def load(self, key, values):
    return self.build(values['thread_key'], key, **values)

  # Updates the threads_messages column family, which indexes messages by
  # their `updated_at` timestamp.  If the Message is being updated, pass
  # the old `updated_at` value for `old_updated` so it can be cleaned up.
  #
  # msg         - The entities.Message that is being reindexed.
  # old_updated - Optional DateTime of the Message's `updated_at` before the
  #               update.
  #
  # Returns nothing.
  def update_msg_index(self, msg, old_updated=None):
    self.th_msgs_fam.insert(msg.thread.key, {(msg.updated_at, msg.key): ''})
    if old_updated:
      self.th_msgs_fam.remove(msg.thread.key, [(old_updated, msg.key)])

  # Gets the range of Message keys for the given Thread.  Cleanup any multiple
  # Message IDs with old timestamps.
  #
  # thread - The entities.Thread to query by.
  #
  # Returns a List of String Message keys.
  def get_unique_msg_keys(self, thread):
    entries = self.th_msgs_fam.get(thread.key, column_count=50)
    keys, dupes = self.filter_dupes(entries)

    if len(dupes) > 0:
      self.th_msgs_fam.remove(thread.key, dupes)

    return keys

  # Partitions the list of entries into two lists: one containing uniques, and
  # one containing the duplicates.
  #
  #   entries = [(datetime(...), UUID()), (datetime(...), UUID())]
  #   keys, dupes = filter_dupes(entries)
  #   keys  # => [UUID().bytes, UUID().bytes]
  #   dupes # => [(datetime(...), UUID()), (datetime(...), UUID())]
  #
  # entries - A List of indexed Messages represented as Tuples, each containing
  #           a DateTime timestmap and a UUID Message key.
  #
  # Returns a Tuple of a List of String UUID byte Message keys and a List of
  # indexed Message Tuples.
  def filter_dupes(self, entries):
    keys     = []
    dupes    = []
    existing = set()
    for timestamp, id in entries:
      bytes = id.bytes
      if bytes in existing:
        dupes.append((timestamp, id))
      else:
        existing.add(bytes)
        keys.append(bytes)
    return (keys, dupes)

class Client:
  def __init__(self, keyspace, **kwargs):
    self.pool     = ConnectionPool(keyspace, **kwargs)
    self.threads  = ThreadClient(self.pool)
    self.messages = MessageClient(self.pool)

