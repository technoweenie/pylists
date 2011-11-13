from lists.client import Client
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

