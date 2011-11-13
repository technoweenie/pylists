import re
import uuid
from datetime import datetime

import pycassa, re
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

import entities

class Client(object):

    def __init__(self, keyspace, **kwargs):
        pool = ConnectionPool(keyspace, **kwargs)
        th_fam = ColumnFamily(pool, 'threads') 
        th_msgs_fam = ColumnFamily(pool, 'thread_messages')
        msgs_fam = ColumnFamily(pool, 'messages')

        self.threads = ThreadClient(self, th_fam, th_msgs_fam)
        self.messages = MessageClient(self, msgs_fam, th_msgs_fam)

        for module in ("uuid", "list", "thread", "msg"):
            setattr(self, module, getattr(entities, "_%s" % module))

class ThreadClient(object):

    def __init__(self, client, th_fam, th_msgs_fam):
        self.client = client 
        self.th_fam = th_fam
        self.th_msgs_fam = th_msgs_fam

    def messages(self, thread):
        """Public: Gets a range of Messages in a Thread.
        
        thread - a lists.Thread instance.
       
        Returns an Array of lists.Message instances.
        """

        thread = self.client.thread(thread)
        keys = self.get_unique_msg_keys(thread)
        return self.client.messages.multiget(keys)

    def get(self, key):
        """Public: Get a Thread.
        
        key - The String Thread key.
        
        Returns an entities.Thread.
        """

        try:
            return self.load(key, self.th_fam.get(key))
        except pycassa.cassandra.c10.ttypes.NotFoundException:
            pass

    def save(self, thread):
        """Public: Stores the Thread in Cassandra.
        
        thread - The entities.Thread to save.
        
        Returns nothing.
        """

        self.th_fam.insert(thread.key, {
            'title': thread.title})

    def load(self, key, values):
        """Builds a new Thread object from a Cassandra result.
        
        key    - The UUID key.
        values - A Dict of Message attributes.
                 title      - The String title.
                 thread_key - The String Thread key.
                 created_at - The DateTime creation timestamp.
                 updated_at - The DateTime modification timestamp.
        
        Returns an entities.Thread.
        """

        return self.client.thread(key, **values)

    def get_unique_msg_keys(self, thread):
        """Gets the range of Message keys for the given Thread.  Cleanup any multiple
        Message IDs with old timestamps.
        
        thread - The entities.Thread to query by.
        
        Returns a List of String Message keys.
        """

        entries = self.th_msgs_fam.get(thread.key, column_count=50)
        keys, dupes = self.filter_dupes(entries)

        if len(dupes) > 0:
            self.th_msgs_fam.remove(thread.key, dupes)

        return keys

    def filter_dupes(self, entries):
        """Partitions the list of entries into two lists: one containing uniques, and
        one containing the duplicates.
        
          entries = [(datetime(...), UUID()), (datetime(...), UUID())]
          keys, dupes = filter_dupes(entries)
          keys  # => [UUID().bytes, UUID().bytes]
          dupes # => [(datetime(...), UUID()), (datetime(...), UUID())]
        
        entries - A List of indexed Messages represented as Tuples, each containing
                  a DateTime timestmap and a UUID Message key.
        
        Returns a Tuple of a List of String UUID byte Message keys and a List of
        indexed Message Tuples.
        """

        keys = []
        dupes = []
        existing = set()
        for timestamp, id in entries:
            bytes = id.bytes
            if bytes in existing:
                dupes.append((timestamp, id))
            else:
                existing.add(bytes)
                keys.append(bytes)

        return (keys, dupes)

class MessageClient(object):

    def __init__(self, client, msgs_fam, th_msgs_fam):
        self.client = client 
        self.msgs_fam = msgs_fam
        self.th_msgs_fam = th_msgs_fam

    def get(self, key):
        """Public: Gets a single Message.
        
        key - String Message UUID.
        
        Returns an entities.Message.
        """

        id = self.client.uuid(key)
        values = self.msgs_fam.get(id.bytes)
        return self.load(id, values)

    def multiget(self, keys):
        msgs = []
        rows = self.msgs_fam.multiget(keys)
        for key in rows:
            id = self.client.uuid(key)
            values = rows[key]
            msgs.append(self.load(id, values))

        return msgs

    def save(self, msg):
        """Public: Stores the Message in Cassandra and updates any indexes.
        
        msg - The entities.Message to save.
        
        Returns nothing.
        """

        old_updated = None
        now = datetime.utcnow()
        if msg.key:
            old_updated = msg.updated_at
        else:
            msg.created_at = now
            msg.key = self.client.uuid()

        msg.updated_at = now
        columns = {
            "thread_key": msg.thread.key, "title": msg.title,
            "created_at": msg.created_at, "updated_at": msg.updated_at}
        self.msgs_fam.insert(msg.key.bytes, columns)
        self.update_msg_index(msg, old_updated)

    def load(self, key, values):
        """Builds a new Thread object from a Cassandra result.
        
        key    - The UUID key.
        values - A Dict of Message attributes.
                 title      - The String title.
                 thread_key - The String Thread key.
                 created_at - The DateTime creation timestamp.
                 updated_at - The DateTime modification timestamp.
        
        Returns an entities.Thread.
        """

        return self.client.msg(values['thread_key'], key, **values)

    def update_msg_index(self, msg, old_updated=None):
        """Updates the threads_messages column family, which indexes messages by
        their `updated_at` timestamp.  If the Message is being updated, pass
        the old `updated_at` value for `old_updated` so it can be cleaned up.
        
        msg         - The entities.Message that is being reindexed.
        old_updated - Optional DateTime of the Message's `updated_at` before the
                      update.
        
        Returns nothing.
        """

        self.th_msgs_fam.insert(msg.thread.key, {(msg.updated_at, msg.key): ''})
        if old_updated:
            self.th_msgs_fam.remove(msg.thread.key, [(old_updated, msg.key)])

