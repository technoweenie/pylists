from lists import Client
from time import sleep
import lists

c = Client("liststest")
a_thread = c.threads.build('test', title="testing")
print a_thread
c.threads.save(a_thread)

a_msg = c.messages.build(a_thread, None, title="some message")

c.messages.save(a_msg)

sleep(1)

a_msg.title = "some message!"
c.messages.save(a_msg)

for msg in c.messages.list(a_thread):
  print msg

