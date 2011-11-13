import uuid

# Initializes a new ThreadEntity or passes one through.
#
#   a_thread    = Thread("yay", title="Yay")
#   same_thread = Thread(a_thread)
#   new_thread  = Thread("new", title="New")
#
# key - Either a String Thread key or a ThreadEntity.
#
# Returns a ThreadEntity.
def Thread(key, **attrs):
  if hasattr(key, 'key'):
    return key
  else:
    return ThreadEntity(key, **attrs)

# Initializes a new MessageEntity or passes one through.
#
#   a_msg    = Message("thread", "yay", title="Yay")
#   same_msg = Message("thread", a_thread)
#   new_msg  = Message("thread", "new", title="New")
#
# thread - Either a String Thread key or a ThreadIdentity
# key    - Either a UUID or String Message key or a MessageEntity.
#
# Returns a MessageEntity.
def Message(thread, key, **attrs):
  if hasattr(key, 'key'):
    return key
  else:
    return MessageEntity(thread, key, **attrs)

# Builds a UUID.
#
#   new_uuid  = UUID()
#   same_uuid = UUID(new_uuid)
#   same_uuid = UUID(new_uuid.bytes) # using 16-char byte string
#   same_uuid = UUID(str(new_uuid))  # using 36-char hex string
#
# value - Optional UUID or String representation of a UUID.
#
# Returns a uuid.UUID.
def UUID(value=None):
  if hasattr(value, 'bytes'):
    return value
  elif value:
    guid = str(value)
    if len(guid) == 16:
      return uuid.UUID(bytes=guid)
    else:
      return uuid.UUID(value)
  else:
    return uuid.uuid1()

class ThreadEntity:
  def __init__(self, key, **attrs):
    self.title = attrs.setdefault('title', None)
    self.key   = key

  def __str__(self):
    return "<Thread %s title=%s>" % (self.key, self.title)

class MessageEntity:
  def __init__(self, thread, key, **attrs):
    self.key        = key and UUID(key) or None
    self.thread     = Thread(thread)
    self.title      = attrs.setdefault('title', None)
    self.created_at = attrs.setdefault('created_at', None)
    self.updated_at = attrs.setdefault('updated_at', None)

  def __str__(self):
    return "<Message %s title=%s>" % (self.key, self.title)
