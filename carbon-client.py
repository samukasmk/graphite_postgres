#!/usr/bin/env python

# Best Union of examples: 
# https://github.com/graphite-project/carbon/blob/master/examples/example-client.py
# https://github.com/graphite-project/graphite-web/blob/master/contrib/demo-collector.py
# http://coreygoldberg.blogspot.com.br/2012/04/python-getting-data-into-graphite-code.html

try:
    import cPickle as pickle
except:
    import pickle as pickle

#from commands import getstatusoutput
from platform import node
from socket import socket, AF_INET, SOCK_STREAM
from sys import argv, exit
from time import sleep, time

DELAY = 60
CARBON_SERVER = 'localhost'
CARBON_PORT = 2003

class Carbon():
	def __init__(self, hostname, port):
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.hostname = hostname
		self.port = int(port)
		self.connect()

	def connect(self):
		try:
			self.socket.connect((self.hostname, self.port))
		#except socket.error, e:
		except IOError, e:
			print "Couldn't connect to (%s) on port (%d), %s,\n" \
					"Is carbon-cache.py running?" % \
						(CARBON_SERVER, CARBON_PORT, str(e))
			return

	def disconnect(self):
		self.socket.close()

	def send_metric(self, metric_string):
		"""
			metric = 'metric_path value timestamp'
			Example:
			>>> metric = 'foo.bar.baz 42 %d\n' % int(time())
			>>> graphiteCarbon.send_metric(metric)
		"""
		try:
			self.socket.sendall(metric_data + "\n")
		except:
			self.connect()
			self.socket.sendall(metric_data + "\n")

	def send_many_metrics(self, listOfMetricTuples):
		"""
			listOfMetricTuples = (path, (timestamp, value)), ...]

			Example:
			>>> listOfMetricTuples = [
			>>> 	('system.%s.loadavg_1min', % node, (timestamp, loadavgs[0])),
			>>> 	('system.%s.loadavg_5min', % node, (timestamp, loadavgs[1])),
			>>> 	('system.%s.loadavg_15min' % node, (timestamp, loadavgs[2]))
			>>> ]

			>>> graphite.send_many_metrics(listOfMetricTuples)
		"""

		payload = pickle.dumps(listOfMetricTuples)
		header = struct.pack("!L", len(payload))
		message = header + payload

		try:
			self.socket.send(message + "\n")
		except:
			self.connect()
			self.socket.send(message + "\n")

def main():
    
    hostname = node().split('.')[0]

    graphite = Carbon(CARBON_SERVER, CARBON_PORT)

	### Simple Example of send just one metric
	# metric = 'foo.bar.baz 42 %d\n' % int(time())
	# graphite.send_metric(metric)


	### Example of send many metrics
	# listOfMetricTuples = [
	# 	('system.%s.loadavg_1min', % hostname, (timestamp, loadavgs[0])),
	# 	('system.%s.loadavg_5min', % hostname, (timestamp, loadavgs[1])),
	# 	('system.%s.loadavg_15min' % hostname, (timestamp, loadavgs[2]))
	# ]
	# graphite.send_many_metrics(listOfMetricTuples)


	### Example of send one metric with loop interval
	# try:
	# 	while True:
	# 		metric = 'foo.bar.baz 42 %d\n' % int(time())
	# 		graphite.send_metric(metric)
	# 		sleep(SEND_DELAY)
	# except KeyboardInterrupt:
	# 	stderr.write("\nExiting on CTRL-c\n")
	# 	exit(0)


	### Example of send many metrics with loop interval:
	# try:
	# 	while True:
	# 		total_results = []
	#
	# 		try:
	# 			result_database_size = checker_postgres.check_database_size()
	# 			total_results = total_results + result_database_size
	# 		except:
	# 			print '[Error]: On get result_database_size'
	#
	# 		try:
	# 			result_database_connections = checker_postgres.check_database_connections()
	# 			total_results = total_results + result_database_connections
	# 		except:
	# 			print '[Error]: On get result_database_connections'
	#
	# 		print total_results
	# 		graphite.send_many_metrics(total_results)
	#
	# 		sleep(SEND_DELAY)
	# except KeyboardInterrupt:
	# 	stderr.write("\nExiting on CTRL-c\n")
	# 	exit(0)

if __name__ == '__main__':
	main()
