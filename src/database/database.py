import os

from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool
import psycopg2.extras

pg_config = {
	'user': os.environ.get('DB_USER', 'postgres'),
	'password': os.environ.get('DB_PASS', ''),
	'dbname': os.environ.get('DB_NAME', 'prices_prod')
}

def connect(host):
	pg_config['host'] = host
	return SimpleConnectionPool(1, 1, **pg_config)

def connect_to_db() :
	try:
		# pg_pool = connect('/cloudsql/panprices:europe-west1:panprices-psql')
		pg_pool = connect('/cloudsql/panprices:europe-west1:panprices-core')
	except OperationalError as e:
		# If production settings fail, use local development ones
		pg_pool = connect('localhost')
	# Remember to close SQL resources declared while running this function.
	# Keep any declared in global scope (e.g. pg_pool) for later reuse.
	with pg_pool.getconn() as connection:
		cur = connection.cursor()
		cur_dict = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
		return [cur, cur_dict, connection, pg_pool]
