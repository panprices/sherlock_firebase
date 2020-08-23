import io
from src.sqlite.sqlite import SQLite
from google.cloud import storage
import threading

def _get_support_table(table) :
	storage_client = storage.Client('panprices')
	bucket = storage_client.bucket('panprices_sherlock')
	blob = bucket.blob('support_tables/' + table + '.csv')
	content = blob.download_as_string()
	return content

def _load_data_to_sqlite_from_gcs(db, table) :
	# Load the table in SQLite's memory
	db.read_csv(
		csv_path=io.BytesIO(_get_support_table(table)),
		table_name=table
	)

def _load_offers_to_sqlite(db, offers) :
	# Load offers
	db.read_json(
		offers,
		'offers'
	)

def _threaded_data_load(db, tables) :
	threads = {}
	for i, table in enumerate(tables) :
		threads[i] = threading.Thread(
			target=_load_data_to_sqlite_from_gcs,
			args=([db, table])
		)
		# Start the thread
		threads[i].start()
	# Join all the threads and make this operation blocking
	for i, table in enumerate(tables) :
		threads[i].join()

def add_offers_metadata(offers) :
	db = SQLite()
	cursor = db.get_cursor()

	_threaded_data_load(
		db,
		['alexa', 'currency', 'retailers', 'shipping', 'trustpilot']
	)
	_load_offers_to_sqlite(db, offers)

	for row in cursor.execute('SELECT * FROM offers') :
		print(row)
