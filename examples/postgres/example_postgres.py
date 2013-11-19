#!/usr/bin/python
#

# Source:
# http://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL
#

import psycopg2
import sys
import pprint

def main():
	conn_string = "host='192.168.5.109' dbname='postgres' user='db_user' password='db_pass'"
	# print the connection string we will use to connect
	print "Connecting to database\n	->"#%s" % (conn_string)
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
 


	query = "SELECT pg_database_size(d.oid) AS dsize, " \
  			"pg_size_pretty(pg_database_size(d.oid)) AS pdsize, " \
  			"datname, " \
  			"usename " \
			"FROM pg_database d " \
			"LEFT JOIN pg_user u ON (u.usesysid=d.datdba) " \
			"ORDER BY 1 DESC "

	# execute our Query
	cursor.execute(query)
 
	# retrieve the records from the database
	records = cursor.fetchall()
 
	# print out the records using pretty print
	# note that the NAMES of the columns are not shown, instead just indexes.
	# for most people this isn't very useful so we'll show you how to return
	# columns as a dictionary (hash) in the next example.
	#pprint.pprint(records)
	for p in records:
		print p
 
if __name__ == "__main__":
	main()