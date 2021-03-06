import pycassa
from pycassa.types import *
from pycassa.system_manager import *

# Completely destroys and recreates the sample keyspace for this app.
def setup(keyspace):
    schema = Schema(keyspace)
    schema.create_keyspace()
    schema.create_column_families()
    schema.close()

class Schema(object):

    def __init__(self, keyspace, **kwargs):
        self.keyspace = keyspace
        self.sys = SystemManager(**kwargs)

    def create_keyspace(self):
        try:
            self.sys.drop_keyspace(self.keyspace)
        except pycassa.cassandra.c10.ttypes.InvalidRequestException:
            pass

        self.sys.create_keyspace(self.keyspace,
            strategy_options={'replication_factor': '1'})

    def create_column_families(self):
        self.create_lists_cf()
        self.create_threads_cf()
        self.create_msgs_cf()
        self.create_list_threads_cf()
        self.create_list_msgs_cf()
        self.create_thread_msgs_cf()

    def close(self):
        self.sys.close()

    def create_lists_cf(self):
        self.sys.create_column_family(self.keyspace, 'lists',
            key_validation_class=UTF8_TYPE,
            comparator_type=UTF8_TYPE)
        self.alter_columns('lists', name=UTF8_TYPE)

    def create_threads_cf(self):
        self.sys.create_column_family(self.keyspace, 'threads',
            key_validation_class=UTF8_TYPE,
            comparator_type=UTF8_TYPE)
        self.alter_columns('threads', list_key=UTF8_TYPE, title=UTF8_TYPE,
            message_updated_at=DATE_TYPE)

    def create_msgs_cf(self):
        self.sys.create_column_family(self.keyspace, 'messages',
            key_validation_class=TimeUUIDType,
            comparator_type=UTF8_TYPE)
        self.alter_columns('messages',
            list_key=UTF8_TYPE, thread_key=UTF8_TYPE,
            title=UTF8_TYPE,
            created_at=DATE_TYPE, updated_at=DATE_TYPE)

    def create_list_threads_cf(self):
        self.sys.create_column_family(self.keyspace, 'list_threads',
            key_validation_class=UTF8_TYPE,
            comparator_type=CompositeType(
                DateType(reversed=True),UTF8_TYPE))

    def create_list_msgs_cf(self):
        self.sys.create_column_family(self.keyspace, 'list_messages',
            key_validation_class=UTF8_TYPE,
            comparator_type=CompositeType(
                DateType(reversed=True),TimeUUIDType()))

    def create_thread_msgs_cf(self):
        self.sys.create_column_family(self.keyspace, 'thread_messages',
            key_validation_class=UTF8_TYPE,
            comparator_type=CompositeType(
                DateType(reversed=True),TimeUUIDType()))

    def alter_columns(self, cf, **columns):
        for name in columns:
            self.sys.alter_column(self.keyspace, cf, name, columns[name])

