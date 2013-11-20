#!/usr/bin/python
#
# Script: graphite_postgres.py
#
# By: Samuel Maciel Sampaio [2013119] <samukasmk@gmail.com>
#
# Objective: monitor postgres database services (by sql querys) and report
#            to graphite (by carbon-cache.py) without statsd or collectd stuffs
#
# Inspired of sources:
# 	http://graphite.readthedocs.org/en/latest/feeding-carbon.html
# 	https://github.com/graphite-project/graphite-web/blob/master/contrib/demo-collector.py
# 	https://github.com/graphite-project/carbon/blob/master/examples/example-client.py
# 	http://coreygoldberg.blogspot.com.br/2012/04/python-getting-data-into-graphite-code.html
# 	http://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL
# 	https://gist.github.com/shawnbutts/3906915

# Postgres 
import psycopg2
import pprint
### Graphite-Carbon
try:
    import cPickle as pickle
except:
    import pickle as pickle
import struct
from platform import node
from socket import socket, AF_INET, SOCK_STREAM
from sys import argv, exit, stderr
from time import sleep, time



POSTGRES_HOST='192.168.5.210'
POSTGRES_DBNAME='postgres'
POSTGRES_USER='postgres'
POSTGRES_PASSWORD='db_pass_here'

SEND_DELAY = 60
CARBON_SERVER = 'localhost'
CARBON_PORT = 2003



class Carbon():
	def __init__(self, hostname, carbon_port_default=2003, carbon_port_pickle_protocol=2004):
		self.socket_default = socket(AF_INET, SOCK_STREAM)
		self.socket_pickle = socket(AF_INET, SOCK_STREAM)
		self.hostname = hostname
		self.carbon_port_default = int(carbon_port_default)
		self.carbon_port_pickle_protocol = int(carbon_port_pickle_protocol)
		#
		# Test connections
		self.test_connection_default()
		self.test_connection_pickle()

	def connect_default(self, carbon_port):
		try:
			self.socket_default = socket(AF_INET, SOCK_STREAM)
			self.socket_default.connect((self.hostname, carbon_port))
		#except socket.error, e:
		except IOError, e:
			print "Couldn't connect to (%s) on port (%d), %s,\n" \
					"Is carbon-cache.py running?" % \
						(self.hostname, carbon_port, str(e))
			return

	def disconnect_all(self):
		print "Disconnecting all sockets..."
		try:
			self.socket_default.close()
			print '\r...OK'
		except:
			print 'Error: on disconnect socket (carbon default)'
		try:
			self.socket_pickle.close()
			print '\r...OK'
		except:
			print 'Error: on disconnect socket (carbon pickle)'

	def connect_pickle(self, carbon_port):
		try:
			self.socket_pickle = socket(AF_INET, SOCK_STREAM)
			self.socket_pickle.connect((self.hostname, carbon_port))
		#except socket.error, e:
		except IOError, e:
			print "Couldn't connect to (%s) on port (%d), %s,\n" \
					"Is carbon-cache.py running?" % \
						(self.hostname, carbon_port, str(e))
			return			

	def test_connection_default(self):
		try: 
			self.connect_default(self.carbon_port_default)
			self.socket_default.close()
			print "CARBON (Default) Connection to (%s) on port (%d) " \
					"is [  OK  ]" % (self.hostname, self.carbon_port_default)			
		except IOError, e:
			print "Couldn't connect to (%s) on port (%d), %s,\n" \
					"Is carbon-cache.py running?" % \
						(self.hostname, carbon_port, str(e))
			return	

	def test_connection_pickle(self):
		try: 
			self.connect_pickle(self.carbon_port_pickle_protocol)
			self.socket_pickle.close()
			print "CARBON (Pickle) Connection to (%s) on port (%d)  " \
					"is [  OK  ]" % (self.hostname, self.carbon_port_pickle_protocol)
		except IOError, e:
			print "Couldn't connect to (%s) on port (%d), %s,\n" \
					"Is carbon-cache.py running?" % \
						(self.hostname, carbon_port, str(e))
			return	

	def send_metric(self, metric_string):
		"""
			metric = 'metric_path value timestamp'
			Example:
			>>> metric = 'foo.bar.baz 42 %d\n' % int(time())
			>>> graphiteCarbon.send_metric(metric)
		"""
		try:
			self.socket_default.sendall(metric_string + "\n")
		except:
			try:
				self.connect_default(carbon_port = self.carbon_port_default)
				self.socket_default.sendall(metric_string + "\n")
			except Exception, e:
				print str(e)


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
			self.socket_pickle.sendall(message + "\n")
		except:
			#### arrumar aqui
			#### socket.error: [Errno 32] Broken pipe
			try:
				self.connect_pickle(carbon_port = self.carbon_port_pickle_protocol)
				self.socket_pickle.sendall(message + "\n")
			except Exception, e:
				print str(e)


class PostgresInterface():

	def __init__(self, host, dbname, user, password):

		# define the conn_string
		self.conn_string = "host='" +host+ "' dbname='" +dbname+ "' user='" +user+ "' password='" +password+ "'"

		# print the connection string we will use to connect
		#print "Connecting to database\n	->"#%s" % (conn_string)

		# get a connection, if a connect cannot be made an exception will be raised here
		self.conn = psycopg2.connect(self.conn_string)

	def run_query_fetchall(self, query):
 		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = self.conn.cursor()

		# execute our Query
		cursor.execute(query)

		# retrieve the records from the database
		return cursor.fetchall()

	def run_query_fetchone(self, query):
 		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = self.conn.cursor()

		# execute our Query
		cursor.execute(query)

		# retrieve the records from the database
		return cursor.fetchone()		

	def run_query_fetchmany(self, query, qty_rows):
 		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = self.conn.cursor()

		# execute our Query
		cursor.execute(query)

		# retrieve the records from the database
		return cursor.fetchmany(qty_rows)
 


class CheckerPostgres():

	def __init__(self, postgres_api):
		pass
		self.postgres_api = postgres_api

	def __bytes_to(self, bytes, to, bsize=1024):
		"""convert bytes to megabytes, etc.
			sample code:
				print('mb= ' + str(bytesto(314575262000000, 'm')))

			sample output:
				mb= 300002347.946
		"""

		a = {'k' : 1, 'm': 2, 'g' : 3, 't' : 4, 'p' : 5, 'e' : 6 }
		r = float(bytes)
		for i in range(a[to.lower()[:1]]):
			r = r / bsize

		return(r)

	def check_database_size(self):

		# Extra (Show Full Options disable)
		#"SELECT pg_database_size(d.oid) AS dsize, " \
		#"pg_size_pretty(pg_database_size(d.oid)) AS pdsize, " \

		query = "SELECT pg_database_size(d.oid) AS dsize, " \
				"datname, " \
				"usename " \
				"FROM pg_database d " \
				"LEFT JOIN pg_user u ON (u.usesysid=d.datdba) " \
				"ORDER BY 1 DESC "

		result = self.postgres_api.run_query_fetchall(query)

		database_size_list = [] 
		for row in result:
			print "postgres.machine.%s.database_size_in_mb %s %d" % \
				(str(row[1]), int(self.__bytes_to(str(row[0]), 'MB')), time())

			database_size_list.append( ("postgres.machine.%s.database_size_in_mb" % (str(row[1])), (int(time()), int(self.__bytes_to(str(row[0]), 'MB')))))

			# metric_path = "postgres.machine."+str(row[1])+".database_size_in_mb"
			# value = int(
			# 	self.__bytes_to(int(row[0]), 'MB')
			# 	)
		
			# database_size_list.append(str(metric_path) +" "+ str(value) +" "+ str(int(time())))

		return database_size_list


def main():

	from configobj import ConfigObj
	config = ConfigObj('config.ini')


	POSTGRES_HOST = str(config['POSTGRES_HOST'])
	POSTGRES_DBNAME = str(config['POSTGRES_DBNAME'])
	POSTGRES_USER = str(config['POSTGRES_USER'])
	POSTGRES_PASSWORD = str(config['POSTGRES_PASSWORD'])
	SEND_DELAY = int(config['SEND_DELAY'])
	CARBON_SERVER = str(config['CARBON_SERVER'])
	#CARBON_PORT = int(config['CARBON_PORT'])
	CARBON_PORT_DEFAULT = int(config['CARBON_PORT_DEFAULT'])
	CARBON_PORT_PICKLE = int(config['CARBON_PORT_PICKLE'])


	postgres_api = PostgresInterface(POSTGRES_HOST, POSTGRES_DBNAME, POSTGRES_USER, POSTGRES_PASSWORD)

	graphite_api = Carbon(CARBON_SERVER, CARBON_PORT_DEFAULT, CARBON_PORT_PICKLE)

	checker_postgres = CheckerPostgres(postgres_api)


	### Simple Example of send just one metric (via string)
	#
	# >>> metric = 'foo.bar.baz 42 %d\n' % int(time())
	# >>>
	# >>> graphite.send_metric(metric)

	### Example of send many metrics (via pickle)
	### [Best Soluction for multiples metric in the same time]
	#
	# >>> listOfMetricTuples = [
	# ...    ('system.machine.loadavg_1min', (str(time()), '122')),
	# ...    ('system.machine.loadavg_5min', (str(time()), '1222')),
	# ...    ('system.machine.loadavg_15min', (str(time()), '12233'))
	# ...    ]
	# >>>
	# >>> graphite_api.send_many_metrics(listOfMetricTuples)

	### Example of send many metrics (via string)
	### [No so good like pickle and serializable objects]
	# >>> metrics_list = [ 'foo.bar.baz.A 42 12233333', 'foo.bar.baz.B 24 12233333', 'foo.bar.baz.B 8172 12233333' ]
	# >>> metrics_string = '\n'.join(metrics_list) + '\n'
	# >>>
	# >>> metrics_string
	# 'foo.bar.baz.A 42 12233333\nfoo.bar.baz.B 24 12233333\nfoo.bar.baz.B 8172 12233333\n'
	# >>>
	# >>> graphite_api.send_metric(metrics_string)



	try:
		while True:

			total_results = []

			try: 
				result_database_size = checker_postgres.check_database_size()
				total_results = total_results + result_database_size
			except Exception, e:
				print 'Error: On get result_database_size, '+str(e)

			try: 
				#result_b = checker_postgres.check_database_size()
				total_results = total_results + result_b
			except:
				print '[Error]: On get result_b'

			graphite_api.send_many_metrics(total_results)

			# total_results.append( ("a.b.c.pickle.AA", ('112','11112221')) )
			# total_results.append( ("a.b.c.pickle.BB", ('112','11112221')) )
			# total_results.append( ("a.b.c.pickle.CC", ('112','11112221')) )
			# graphite_api.send_many_metrics(total_results)
			# print total_results

			# metrics_list = [ 'foo.bar.baz.A 42 12233333', 'foo.bar.baz.B 24 12233333', 'foo.bar.baz.B 8172 12233333' ]
			# metrics_string = '\n'.join(metrics_list) + '\n'
			# graphite_api.send_metric(metrics_string)
			# print metrics_string

			sleep(SEND_DELAY)

	except KeyboardInterrupt:
		stderr.write("\n\nExiting on CTRL-c\n")
		graphite_api.disconnect_all()
		exit(0)



if __name__ == "__main__":
	main()

