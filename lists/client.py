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
        lst_fam = ColumnFamily(pool, 'lists') 
        th_fam = ColumnFamily(pool, 'threads') 
        lst_threads_fam = ColumnFamily(pool, 'list_threads')
        lst_msgs_fam = ColumnFamily(pool, 'list_messages')
        th_msgs_fam = ColumnFamily(pool, 'thread_messages')
        msgs_fam = ColumnFamily(pool, 'messages')

        self.lists = ListClient(self, lst_fam, lst_threads_fam, lst_msgs_fam)
        self.threads = ThreadClient(self, th_fam, lst_threads_fam,
            th_msgs_fam)
        self.messages = MessageClient(self, msgs_fam)

        for module in ("uuid", "list", "thread", "msg"):
            setattr(self, module, getattr(entities, "_%s" % module))

class ListClient(object):

    def __init__(self, client, lst_fam, lst_threads_fam, lst_msgs_fam):
        self.client = client
        self.column_fam = lst_fam
        self.lst_threads_fam = lst_threads_fam
        self.lst_msgs_fam = lst_msgs_fam

    def threads(self, lst):
        """Public: Gets a range of Threads in a List.
        
        lst - a lists.List instance.
       
        Returns an Array of lists.List instances.
        """

        lst = self.client.list(lst)
        keys = get_unique_msg_keys(self.lst_threads_fam, lst.key)
        return self.client.threads.multiget(keys)

    def messages(self, lst):
        """Public: Gets a range of Messages in a List.
        
        lst - a lists.List instance.
       
        Returns an Array of lists.Message instances.
        """

        lst = self.client.list(lst)
        keys = get_unique_msg_keys(self.lst_msgs_fam, lst.key, uuidbytes)
        return self.client.messages.multiget(keys)

    def get(self, key):
        """Public: Get a List.
        
        key - The String List key.
        
        Returns an entities.List.
        """

        try:
            return self.load(key, self.column_fam.get(key))
        except pycassa.cassandra.c10.ttypes.NotFoundException:
            pass

    def save(self, lst):
        """Public: Stores the List in Cassandra.
        
        thread - The entities.List to save.
        
        Returns nothing.
        """

        self.column_fam.insert(lst.key, {
            'name': lst.name})

    def load(self, key, values):
        """Builds a new List object from a Cassandra result.
        
        key    - The UUID key.
        values - A Dict of Message attributes.
                 name - The String title.
        
        Returns an entities.List.
        """

        return self.client.list(key, **values)

    def update_timestamp_index(self, msg, old_updated):
        """Updates the List related timestamp indexes after a message has
        been updated.

        msg         - An entities.Message.
        old_updated - Optional DateTime of the entity's `updated_at` before the
                      update.

        Returns nothing.
        """

        update_timestamp_index(self.lst_msgs_fam,
            msg.list.key, msg, old_updated)
        update_timestamp_index(self.lst_threads_fam,
            msg.list.key, msg.thread, old_updated, 'message_updated_at')

class ThreadClient(object):

    def __init__(self, client, th_fam, lst_threads_fam, th_msgs_fam):
        self.client = client 
        self.column_fam = th_fam
        self.lst_threads_fam = lst_threads_fam
        self.th_msgs_fam = th_msgs_fam

    def messages(self, thread):
        """Public: Gets a range of Messages in a Thread.
        
        thread - a lists.Thread instance.
       
        Returns an Array of lists.Message instances.
        """

        thread = self.client.thread(thread)
        keys = get_unique_msg_keys(self.th_msgs_fam, thread.key, uuidbytes)
        return self.client.messages.multiget(keys)

    def get(self, key):
        """Public: Get a Thread.
        
        key - The String Thread key.
        
        Returns an entities.Thread.
        """

        try:
            return self.load(key, self.column_fam.get(key))
        except pycassa.cassandra.c10.ttypes.NotFoundException:
            pass

    def multiget(self, keys):
        """Public: Gets a list of Messages.

        keys - A List of String Message UUIDs.

        Returns a List of entities.Message instances.
        """

        return multiget(self, keys)

    def save(self, thread):
        """Public: Stores the Thread in Cassandra.
        
        thread - The entities.Thread to save.
        
        Returns nothing.
        """

        values = {'list_key': thread.list.key,
            'title': thread.title}
        if thread.message_updated_at:
            values['message_updated_at'] = thread.message_updated_at
        self.column_fam.insert(thread.key, values)

    def load(self, key, values):
        """Builds a new Thread object from a Cassandra result.
        
        key    - The UUID key.
        values - A Dict of Message attributes.
                 title    - The String title.
                 list_key - The String List key.
        
        Returns an entities.Thread.
        """

        return self.client.thread(values['list_key'], key, **values)

    def update_timestamp_index(self, msg, old_updated):
        """Updates the Thread related timestamp indexes after a message has
        been updated.

        msg         - An entities.Message.
        old_updated - Optional DateTime of the entity's `updated_at` before the
                      update.

        Returns nothing.
        """

        update_timestamp_index(self.th_msgs_fam,
            msg.thread.key, msg, old_updated)

        now = msg.thread.message_updated_at = datetime.utcnow()
        self.column_fam.insert(msg.thread.key, {"message_updated_at": now})

        self.client.lists.update_timestamp_index(msg, old_updated)

class MessageClient(object):

    def __init__(self, client, msgs_fam):
        self.client = client 
        self.column_fam = msgs_fam

    def get(self, key):
        """Public: Gets a single Message.
        
        key - String Message UUID.
        
        Returns an entities.Message.
        """

        id = self.client.uuid(key)
        values = self.column_fam.get(id.bytes)
        return self.load(id, values)

    def multiget(self, keys):
        """Public: Gets a list of Messages.

        keys - A List of String Message UUIDs.

        Returns a List of entities.Message instances.
        """

        return multiget(self, keys)

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
            "list_key": msg.list.key, "thread_key": msg.thread.key,
            "title": msg.title,
            "created_at": msg.created_at, "updated_at": msg.updated_at}
        self.column_fam.insert(msg.key.bytes, columns)

        self.client.threads.update_timestamp_index(msg, old_updated)

    def load(self, key, values):
        """Builds a new Thread object from a Cassandra result.
        
        key    - The UUID key.
        values - A Dict of Message attributes.
                 title      - The String title.
                 list_key   - The String List key.
                 thread_key - The String Thread key.
                 created_at - The DateTime creation timestamp.
                 updated_at - The DateTime modification timestamp.
        
        Returns an entities.Thread.
        """

        key = self.client.uuid(key)
        thread = self.client.thread(values['list_key'], values['thread_key'])
        return self.client.msg(thread, key, **values)

def multiget(client, keys):
    """Handles a multiget against a column familiy.

    client  - The *Client instance.
    keys    - A List of String row keys.

    Returns a List of entities.
    """

    msgs = []
    rows = client.column_fam.multiget(keys)
    for key in rows:
        values = rows[key]
        msgs.append(client.load(key, values))

    return msgs

def update_timestamp_index(column_fam, key, entity, old_updated=None,
        updated_attr='updated_at'):
    """Updates the a column family used strictly for indexing by timestamp.
    If the Message is being updated, pass the old `updated_at` value for 
    `old_updated` so it can be cleaned up.
    
    column_fam   - The ColumnFamily that is being updated.
    key          - The String row key.
    entity       - An entities.* instance.
    old_updated  - Optional DateTime of the entity's `updated_at` before the
                   update.
    updated_attr - The String timestamp column name.  Default: "updated_at".
    
    Returns nothing.
    """

    updated = getattr(entity, updated_attr)
    column_fam.insert(key, {(updated, entity.key): ''})
    if old_updated:
        column_fam.remove(key, [(old_updated, entity.key)])

def get_unique_msg_keys(column_fam, key, filter_comparator=None):
    """Gets the range of Message keys for the given Thread.  Cleanup any multiple
    Message IDs with old timestamps.
    
    column_fam        - The ColumnFamily that is being queried.
    key               - The String row key.
    filter_comparator - Function applied to IDs before being returned in the
                        unique List of keys.  Default: str().
    
    Returns a List of String Message keys.
    """

    entries = column_fam.get(key, column_count=50)
    keys, dupes = filter_dupes(entries, filter_comparator)

    if len(dupes) > 0:
        column_fam.remove(key, dupes)

    return keys

def filter_dupes(entries, id_comparator=None):
    """Partitions the list of entries into two lists: one containing uniques, and
    one containing the duplicates.
    
      entries = [(datetime(...), UUID()), (datetime(...), UUID())]
      keys, dupes = filter_dupes(entries)
      keys  # => [UUID().bytes, UUID().bytes]
      dupes # => [(datetime(...), UUID()), (datetime(...), UUID())]
    
    entries       - A List of indexed Messages represented as Tuples, each
                    containing a DateTime timestmap and a UUID Message key.
    id_comparator - Function applied to IDs before being returned in the List
                    of keys.  Default: str()
    
    Returns a Tuple of a List of String UUID byte Message keys and a List of
    indexed Message Tuples.
    """

    if id_comparator == None:
        id_comparator = str

    keys = []
    dupes = []
    existing = set()
    for timestamp, id in entries:
        if id in existing:
            dupes.append((timestamp, id))
        else:
            existing.add(id)
            keys.append(id_comparator(id))

    return (keys, dupes)

def uuidbytes(uuid):
    if hasattr(uuid, 'bytes'):
        return uuid.bytes
    else:
        return str(uuid)

