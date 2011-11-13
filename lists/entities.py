import uuid

def _thread(key, **attrs):
    """Initializes a new Thread or passes one through.

    a_thread    = _thread("yay", title="Yay")
    same_thread = _thread(a_thread)
    new_thread  = _thread("new", title="New")

    key - Either a String Thread key or a Thread.

    Returns a Thread.
    """

    if hasattr(key, 'key'):
        return key
    else:
        return Thread(key, **attrs)

def _msg(*args, **attrs):
    """Initializes a new Message or passes one through.

    a_msg    = _msg("thread", "yay", title="Yay")
    same_msg = _msg("thread", a_msg)
    same_msg = _msg(a_msg)
    new_msg  = _msg("thread", "new", title="New")

    thread - Either a String Thread key or a Thread.
    key    - Either a UUID or String Message key or a Message.

    Returns a MessageEntity.
    """

    arg_len = len(args)
    if arg_len == 2:
        thread, key = args
        if hasattr(key, 'key'):
            return key
        else:
            return Message(thread, key, **attrs)

    elif arg_len == 1:
        thread_or_msg = args[0]
        if hasattr(thread_or_msg, 'thread'):
            return thread_or_msg
        else:
            return Message(thread_or_msg, None, **attrs)

    else:
        return Message()


def _uuid(value=None):
    """Builds a UUID.

      new_uuid  = _uuid()
      same_uuid = _uuid(new_uuid)
      same_uuid = _uuid(new_uuid.bytes) # using 16-char byte string
      same_uuid = _uuid(str(new_uuid))  # using 36-char hex string

    value - Optional UUID or String representation of a UUID.

    Returns a uuid.UUID.
    """

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

class Thread(object):

    def __init__(self, key, **attrs):
        self.title = attrs.setdefault('title', None)
        self.key = key

    def __str__(self):
        return "<Thread %s title=%s>" % (self.key, self.title)

class Message(object):

    def __init__(self, thread, key, **attrs):
        self.key = key and _uuid(key) or None
        self.thread = _thread(thread)
        self.title = attrs.setdefault('title', None)
        self.created_at = attrs.setdefault('created_at', None)
        self.updated_at = attrs.setdefault('updated_at', None)

    def __str__(self):
        return "<Message %s title=%s>" % (self.key, self.title)
