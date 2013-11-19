#!/usr/bin/env python

# Source: 
# http://coreygoldberg.blogspot.com.br/2012/04/python-getting-data-into-graphite-code.html
#


#
# $ python simple_socket_with_args.py foo.bar.baz 42


import argparse
import socket
import time


CARBON_SERVER = '0.0.0.0'
CARBON_PORT = 2003


parser = argparse.ArgumentParser()
parser.add_argument('metric_path')
parser.add_argument('value')
args = parser.parse_args()


if __name__ == '__main__':
    timestamp = int(time.time())
    message = '%s %s %d\n' % (args.metric_path, args.value, timestamp)
    
    print 'sending message:\n%s' % message
    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))
    sock.sendall(message)
    sock.close()