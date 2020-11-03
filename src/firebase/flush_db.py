from firebase_admin import credentials
from firebase_admin import db
import firebase_admin
from datetime import datetime, timedelta

def delete_data(bucket, hours_cutoff, firebase_db) :
	# Get the timestamp of data and time 1 hour ago
	cutoff = datetime.now() - timedelta(hours = hours_cutoff)
	# The above returns something like 1598616388.782356 get rounded 12 digits
	cutoff_ts = int(float(cutoff.timestamp()) * 1000)
	# Get all the past searches which are older than 1 hour
	items_to_remove = firebase_db \
		.reference(bucket) \
		.order_by_child('created_at') \
		.end_at(cutoff_ts) \
		.get()
	print(f"Delete {len(items_to_remove)} items in {bucket} bucket")
	# Iterate over all the paths and delete them one by one.
	# This is obviously an O(n) operation which is not ideal.
	for node in items_to_remove.keys():
		try :
			print('Deleting the data for ' + bucket + ' bucket: ', node)
			# Request from the Firebase API that the specific search query
			# be deleted.
			firebase_db.reference(bucket).child(node).delete()
		except Exception as e:
			raise e
	return
