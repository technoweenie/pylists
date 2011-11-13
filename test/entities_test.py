from ..lists import entities

from nose.tools import assert_equal, assert_not_equal

def test_thread():
    a_thread = entities._thread("yay", title="Yay")
    assert_equal("yay", a_thread.key)
    assert_equal("Yay", a_thread.title)

    assert_equal(a_thread, entities._thread(a_thread))

    new_thread = entities._thread("boo", title="Boo")
    assert_not_equal(a_thread, new_thread)

def test_message():
    uuid = entities._uuid()
    a_msg = entities._msg("yay", uuid, title="Message")
    assert_equal("yay", a_msg.thread.key)
    assert_equal(uuid, a_msg.key)
    assert_equal("Message", a_msg.title)

    assert_equal(a_msg, entities._msg(a_msg))
    assert_equal(a_msg, entities._msg(None, a_msg))

    thread_msg = entities._msg(a_msg.thread)
    assert_equal(a_msg.thread, thread_msg.thread)
    assert_not_equal(a_msg, thread_msg)

    new_msg = entities._msg("yay", entities._uuid())
    assert_not_equal(a_msg, new_msg)

def test_uuid():
    uuid = entities._uuid()
    assert_equal(16, len(uuid.bytes))
    assert_equal(36, len(str(uuid)))

    assert_equal(uuid, entities._uuid(uuid))
    assert_equal(uuid, entities._uuid(uuid.bytes))
    assert_equal(uuid, entities._uuid(str(uuid)))

