from lists.client import Client

from datetime import datetime
from time import sleep

# Just a simple test that creates a Thread, a new Message, and lists all
# messages in that Thread.

c = Client("liststest")
a_list = c.list("foo@bar.com", name="Foo")

a_thread = c.thread(a_list, 'test', title="testing")

print a_list
print a_thread
print

c.lists.save(a_list)
c.threads.save(a_thread)

a_msg = c.msg(a_thread, title="some message")

c.messages.save(a_msg)

if c.threads.get("existing") == None:
    existing = c.thread(a_list, 'existing', title="Existing")
    existing.message_updated_at = datetime(2010, 1, 1)
    c.threads.save(existing)
    c.messages.save(c.msg(existing, title="Existing Message"))

print "messages for the list"
for msg in c.lists.messages(a_list):
  print msg
print

print "updating..."
sleep(1)
print

a_msg.title = "some message!"
c.messages.save(a_msg)

print "messages for the thread"
for msg in c.threads.messages(a_thread):
    print msg
print

print "threads"
for thread in c.lists.threads(a_list):
    print '%s (%s)' % (thread, thread.message_updated_at)

