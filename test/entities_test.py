from ..lists import entities

from nose.tools import assert_equal, assert_not_equal

def test_list():
    a_list = entities._list('foo@bar.com', name="Foo")
    assert_equal("foo@bar.com", a_list.key)
    assert_equal("Foo", a_list.name)

    assert_equal(a_list, entities._list(a_list))

    new_list = entities._list("bar@bar.com")
    assert_not_equal(a_list, new_list)

def test_thread():
    a_thread = entities._thread("foo@bar.com", "yay", title="Yay")
    assert_equal("foo@bar.com", a_thread.list.key)
    assert_equal("yay", a_thread.key)
    assert_equal("Yay", a_thread.title)

    assert_equal(a_thread, entities._thread(a_thread))

    new_thread = entities._thread("boo", title="Boo")
    assert_not_equal(a_thread, new_thread)
    assert_equal("boo", new_thread.list.key)
    assert_equal(None, new_thread.key)

    new_thread = entities._thread("foo@bar.com", "boo", title="Boo")
    assert_not_equal(a_thread, new_thread)
    assert_equal("boo", new_thread.key)
    assert_equal("foo@bar.com", new_thread.list.key)

    new_thread = entities._thread("foo@bar.com/boo", title="Boo")
    assert_not_equal(a_thread, new_thread)
    assert_equal("boo", new_thread.key)
    assert_equal("foo@bar.com", new_thread.list.key)

def test_message():
    uuid = entities._uuid()
    a_msg = entities._msg("foo@bar.com/yay", uuid, title="Message")
    assert_equal("foo@bar.com", a_msg.list.key)
    assert_equal("yay", a_msg.thread.key)
    assert_equal(uuid, a_msg.key)
    assert_equal("Message", a_msg.title)

    assert_equal(a_msg, entities._msg(a_msg))
    assert_equal(a_msg, entities._msg(None, a_msg))

    thread_msg = entities._msg(a_msg.thread)
    assert_equal(a_msg.thread, thread_msg.thread)
    assert_not_equal(a_msg, thread_msg)

    new_msg = entities._msg("yay", entities._uuid())
    assert_equal("yay", new_msg.thread.list.key)
    assert_not_equal(a_msg, new_msg)

def test_message_with_list():
    lst = entities._list("foo@bar.com", name="foo")
    thread = entities._thread(lst, "bar")
    msg = entities._msg(thread, title="New Message")

    assert_equal("New Message", msg.title)
    assert_equal("bar", msg.thread.key)
    assert_equal("foo", msg.list.name)

    lst.name = "Foo"
    assert_equal("Foo", msg.list.name)

def test_uuid():
    uuid = entities._uuid()
    assert_equal(16, len(uuid.bytes))
    assert_equal(36, len(str(uuid)))

    assert_equal(uuid, entities._uuid(uuid))
    assert_equal(uuid, entities._uuid(uuid.bytes))
    assert_equal(uuid, entities._uuid(str(uuid)))

