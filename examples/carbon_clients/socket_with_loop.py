#!/usr/bin/env python

# Source: 
# http://coreygoldberg.blogspot.com.br/2012/04/python-getting-data-into-graphite-code.html
#
 
import platform
import socket
import time


CARBON_SERVER = '0.0.0.0'
CARBON_PORT = 2003
DELAY = 15  # secs


def get_loadavgs():
    with open('/proc/loadavg') as f:
        return f.read().strip().split()[:3]


def send_msg(message):
    print 'sending message:\n%s' % message
    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))
    sock.sendall(message)
    sock.close()


if __name__ == '__main__':
    node = platform.node().replace('.', '-')
    while True:
        timestamp = int(time.time())
        loadavgs = get_loadavgs()
        lines = [
            'system.%s.loadavg_1min %s %d' % (node, loadavgs[0], timestamp),
            'system.%s.loadavg_5min %s %d' % (node, loadavgs[1], timestamp),
            'system.%s.loadavg_15min %s %d' % (node, loadavgs[2], timestamp)
        ]
        message = '\n'.join(lines) + '\n'
        send_msg(message)
        time.sleep(DELAY)