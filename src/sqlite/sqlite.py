import json
import sqlite3
import pandas
import csv

class SQLite() :

	def __init__(self) :
		# Open up a sqlite database session in memory
		self.conn = sqlite3.connect(
			":memory:",
			check_same_thread=False # This allows for threads
		)
		# Open a cursor
		self.c = self.conn.cursor()

	def get_cursor(self) :
		return self.c

	def read_csv(self, csv_path, table_name) :
		df = pandas.read_csv(
			csv_path,
			error_bad_lines=False,
			encoding='latin-1'
		)
		# Transfer the panda dataframe to a table in memory in sqlite
		df.to_sql(
			table_name,
			self.conn, 
			if_exists='append', 
			index=False
		)

	def read_json(self, json, table_name) :
		df = pandas.DataFrame(json)
		# Transfer the panda dataframe to a table in memory in sqlite
		df.to_sql(
			table_name,
			self.conn, 
			if_exists='append', 
			index=False
		)		

	def close_db() :
		self.conn.close()
