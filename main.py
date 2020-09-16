import base64
import json
import firebase_admin
from datetime import datetime, timedelta
from firebase_admin import credentials
from firebase_admin import db

from src.pubsub.pubsub import Publisher
from src.helpers.helpers import format_search_offer_msg
from src.enricher.enricher import add_offers_metadata

def offer_search_trigger(event, context, production=True):

	"""
		Triggered whenever there is a new database entry on the
		offerSearch resource in the Firebase Realtime Database.
		Panprices web client generates the creation of new database
		entries in this directory.

		The sole purpose of this function is to simply publish an event
		on PubSub so that we trigger Sherlock.
	"""

	# Print out the entire event object
	print('Publishing the following live search for product: ', str(event))
	# Publish the event to the sherlock_products Pubsub topic
	if production :
		try :
			publisher = Publisher('panprices', 'sherlock_products')
			pub_results = publisher.publish_messages([event['delta']])
			publisher_popular_products = Publisher('panprices', 'sherlock_popular_products')
			pub_results_2 = publisher_popular_products.publish_messages([event['delta']])
		except Exception as e :
			raise e
		print(pub_results)
		return pub_results

def live_search_offer_enricher(event, context, production=True) :
	"""
		Consumes messages on the topic 'live_search_offers', enriches them
		and then updates the Firebase Realtime Database with the output.
	"""
	try :
		payload = json.loads(base64.b64decode(event['data']))
		print('Got offers for search_id: ', payload['gtin'])
		# Fetch the service account key JSON file contents
		cred = credentials.Certificate('firebase_service_account.json')
		# Initialize the app with a service account, granting admin privileges
		app = firebase_admin.initialize_app(cred, {
			'databaseURL': 'https://panprices.firebaseio.com/'
		})
		# Open a connection to the database
		ref = db.reference('offers')
		# Choose the relevant search
		search_ref = ref.child(str(payload['gtin']))
		# Get the existing offers data, on this we need to calculate savings
		fetch_ref = search_ref.get('fetchedOffers')
		# Join existing and new offers together to a list (if existing data exists)
		'''
			CASES TO CHECK FOR HERE:
			 - If there are no offers Firebase will return this weird tuple:
			 (None, 'null_etag') This happens in test.
			 - If the client has created a message and this function gets triggered
			 in production we will return something like {createdAt:X, gtin:Y, offers:Z}
			 but the key fetchedOffers won't be in there because that property gets
			 set by this function the first time.
		'''
		if list(fetch_ref)[0] != None and 'fetchedOffers' in fetch_ref[0] :
			existing_offers = fetch_ref[0]['fetchedOffers']
			all_offers = existing_offers + payload['offers']
		else :
			all_offers = payload['offers']
		# Enrich and format all the combined offers
		enriched_offers = add_offers_metadata(all_offers)
		# Update the specific search in Firebase RTD with the newly fetched offers
		if production :
			search_ref.update({
				'fetchedOffers/': enriched_offers,
				'fetchedSources/' + payload['offer_source']: True
			})
			print("Enriched " + "offers/" + str(payload['gtin']) + " with offers.")
		# Kill the connection, otherwise the next instance trying to connect will crash
		firebase_admin.delete_app(app)
	except Exception as e:
		msg_string = json.dumps(payload)
		print(f'something went wrong when handling message: {msg_string}')
		raise e

def product_search_trigger(event, context, production=True):

	"""
		Triggered whenever there is a new database entry on the
		productSearch resource in the Firebase Realtime Database.
		Panprices web client generates the creation of new database
		entries in this directory.

		The sole purpose of this function is to simply publish an event
		on PubSub so that we trigger Google Shopping and later be able
		to return the results back down to the client.
	"""

	# Print out the entire event object
	print('Publishing the following search for a product: ', str(event))
	# Publish the event to the sherlock_products Pubsub topic
	if production :
		try :
			publisher = Publisher('panprices', 'sherlock_google_shopping_test')
			pub_results = publisher.publish_messages([event['delta']])
		except Exception as e :
			raise e
		print(pub_results)
		return pub_results

def product_search_publish_result(event, context, production=True):
	"""
		Listens to changes on pubsub output topic from google shopping
		and writes final results to firebase. If there are previous results
		on the search the function appends. If a gtin appears again, it gets
		overwritten.
	"""

	try:
		payload = json.loads(base64.b64decode(event['data']))
		# Fetch the service account key JSON file contents
		cred = credentials.Certificate('firebase_service_account.json')
		# Initialize the app with a service account, granting admin privileges
		app = firebase_admin.initialize_app(cred, {
			'databaseURL': 'https://panprices.firebaseio.com/'
		})
		# Open a connection to the database
		ref = db.reference('productSearch')
		# Choose the relevant search
		current_entry_ref = ref.child(str(payload['searchQuery']))
		# Get existing data in this specific search
		current_entry = current_entry_ref.get({"results"})[0]
		result = {}
		# If results exist add to existing results dict
		if current_entry != None and "results" in current_entry:
			result = current_entry["results"]
		# Overwrites if gtin id already exists
		result[payload["gtin"]] = payload
		# Remove some fields we don't want to present to the client
		del result[payload["gtin"]]["product_url"]
		del result[payload["gtin"]]["source"]
		del result[payload["gtin"]]["img_encoded"]
		# Only publish if we are running in production
		if production :
			# Update the specific search in Firebase RTD with the newly fetched offers
			current_entry_ref.update({
				"results": result
			})
		# Kill the connection, otherwise the next instance trying to connect will crash
		firebase_admin.delete_app(app)
	except Exception as e:
		raise e

def sherlock_shopping_finish_signal(event, context, production=True) :
	"""
		Listens to changes on pubsub finish topic from google shopping
		and updates the firebase object for the specific search query.
		This way the client will now when to render a message saying
		that the search never resulted in any valid matches with GTIN
		which is important for the user experience.
	"""
	try :
		payload = json.loads(base64.b64decode(event['data']))
		# Fetch the service account key JSON file contents
		cred = credentials.Certificate('firebase_service_account.json')
		# Initialize the app with a service account, granting admin privileges
		app = firebase_admin.initialize_app(cred, {
			'databaseURL': 'https://panprices.firebaseio.com/'
		})
		# Open a connection to the database
		ref = db.reference('productSearch')
		# Choose the relevant search
		current_entry_ref = ref.child(str(payload['searchQuery']))
		# Only publish if we are running in production
		if production :
			# Update the specific search in Firebase RTD with the newly fetched offers
			current_entry_ref.update({
				"searchCompleted": True
			})
		# Kill the connection, otherwise the next instance trying to connect will crash
		firebase_admin.delete_app(app)
	except Exception as e:
		raise e

def delete_old_firebase_data(event, context) :
	try:
		payload = json.loads(base64.b64decode(event['data']))
		# Fetch the service account key JSON file contents
		cred = credentials.Certificate('firebase_service_account.json')
		# Initialize the app with a service account, granting admin privileges
		app = firebase_admin.initialize_app(cred, {
			'databaseURL': 'https://panprices.firebaseio.com/'
		})
		# Get the timestamp of data and time 1 hour ago
		cutoff = datetime.now() - timedelta(hours = 1)
		# The above returns something like 1598616388.782356 get rounded 12 digits
		cutoff_ts = int(float(cutoff.timestamp()) * 1000)
		# Get all the past searches which are older than 1 hour
		items_to_remove = db \
			.reference('productSearch') \
			.order_by_child('createdAt') \
			.end_at(cutoff_ts) \
			.get()
		# Iterate over all the paths and delete them one by one.
		# This is obviously an O(n) operation which is not ideal.
		for search_query in items_to_remove.keys():
			try :
				print('Deleting the dat for search query path: ', search_query)
				# Request from the Firebase API that the specific search query
				# be deleted.
				db.reference('productSearch').child(search_query).delete()
			except Exception as e:
				raise e
		return
	except Exception as e:
		print("There was an error: ", e)
		raise e
