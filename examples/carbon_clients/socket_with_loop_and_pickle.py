#!/usr/bin/env python

# Source: 
# http://coreygoldberg.blogspot.com.br/2012/04/python-getting-data-into-graphite-code.html
# and
# http://graphite.readthedocs.org/en/latest/feeding-carbon.html

try:
    import cPickle as pickle
except:
    import pickle as pickle
 
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

        listOfMetricTuples = [
            ('system.%s.loadavg_1min', % node, (timestamp, loadavgs[0])),
            ('system.%s.loadavg_5min', % node, (timestamp, loadavgs[1])),
            ('system.%s.loadavg_15min' % node, (timestamp, loadavgs[2]))
        ]

        payload = pickle.dumps(listOfMetricTuples)
        header = struct.pack("!L", len(payload))
        message = header + payload

        send_msg(message)
        time.sleep(DELAY)