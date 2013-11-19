#!/usr/bin/env python

# Source: 
# http://coreygoldberg.blogspot.com.br/2012/04/python-getting-data-into-graphite-code.html
#

# To send data, you create a socket connection to the graphite/carbon
# server and send a message (string) in the format:

# "metric_path value timestamp\n"

# -> `metric_path`: arbitrary namespace containing substrings delimited by dots.
#                   The most general name is at the left and the most specific
#                   is at the right.
# -> `value`: numeric value to store.
# -> `timestamp`: epoch time.
#
# messages must end with a trailing newline.
# multiple messages maybe be batched and sent in a single socket operation.
# each message is delimited by a newline, with a trailing newline at the end
# of the message batch.

import socket
import time


CARBON_SERVER = '0.0.0.0'
CARBON_PORT = 2003


message = 'foo.bar.baz 42 %d\n' % int(time.time())


print 'sending message:\n%s' % message
sock = socket.socket()
sock.connect((CARBON_SERVER, CARBON_PORT))
sock.sendall(message)
sock.close()