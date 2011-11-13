import pycassa
from pycassa.types import *
from pycassa.system_manager import *

class Schema:
  def __init__(self, keyspace, **kwargs):
    self.keyspace = keyspace
    self.sys      = SystemManager(**kwargs)

  def create_keyspace(self):
    try:
      self.sys.drop_keyspace(self.keyspace)
    except pycassa.cassandra.c10.ttypes.InvalidRequestException:
      pass
    self.sys.create_keyspace(self.keyspace,
        strategy_options={'replication_factor': '1'})

  def create_column_families(self):
    self.create_threads_cf()
    self.create_msgs_cf()
    self.create_thread_msgs_cf()

  def create_threads_cf(self):
    self.sys.create_column_family(self.keyspace, 'threads',
      key_validation_class=UTF8_TYPE,
      comparator_type=UTF8_TYPE)
    self.alter_columns('threads', title=UTF8_TYPE)

  def close(self):
    self.sys.close()

  def create_msgs_cf(self):
    self.sys.create_column_family(self.keyspace, 'messages',
      key_validation_class=TimeUUIDType,
      comparator_type=UTF8_TYPE)
    self.alter_columns('messages',
      title      = UTF8_TYPE,
      thread_key = UTF8_TYPE,
      created_at = DATE_TYPE,
      updated_at = DATE_TYPE)

  def create_thread_msgs_cf(self):
    self.sys.create_column_family(self.keyspace, 'thread_messages',
      key_validation_class=UTF8_TYPE,
      comparator_type=CompositeType(
        DateType(reversed=True),TimeUUIDType()))

  def alter_columns(self, cf, **columns):
    for name in columns:
      self.sys.alter_column(self.keyspace, cf, name, columns[name])
