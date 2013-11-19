#!/usr/bin/python
#

# Postgres 
import psycopg2
import pprint
### Graphite-Carbon
try:
    import cPickle as pickle
except:
    import pickle as pickle

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
			#print "postgres.machine.%s.database_size_in_mb %s %d" % \
			#	(str(row[1]), int(self.__bytes_to(str(row[0]), 'MB')), time())
			database_size_list.append( ("postgres.machine.%s.database_size_in_mb" % (str(row[1])), (int(time()), int(self.__bytes_to(str(row[0]), 'MB')))))

		return database_size_list


def main():

	postgres_api = PostgresInterface(POSTGRES_HOST, POSTGRES_DBNAME, POSTGRES_USER, POSTGRES_PASSWORD)

	graphite_api = Carbon(CARBON_SERVER, CARBON_PORT)

	checker_postgres = CheckerPostgres(postgres_api)


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

	try:
		while True:

			total_results = []

			try: 
				result_a = checker_postgres.check_database_size()
				total_results = total_results + result_a
			except:
				print '[Error]: On get result_a'

			try: 
				#result_b = checker_postgres.check_database_size()
				total_results = total_results + result_b
			except:
				print '[Error]: On get result_b'

			print total_results
			#graphite.send_many_metrics(total_results)			

			sleep(SEND_DELAY)

	except KeyboardInterrupt:
		stderr.write("\nExiting on CTRL-c\n")
		exit(0)



if __name__ == "__main__":
	main()
