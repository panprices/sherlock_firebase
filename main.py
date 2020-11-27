import base64
import json
import firebase_admin
import time
from firebase_admin import credentials
from firebase_admin import db
from flask import jsonify

import src.helpers.encryption as encryption
from src.pubsub.pubsub import Publisher
from src.helpers.helpers import format_search_offer_msg
from src.enricher.enricher import add_offers_metadata
from src.firebase import flush_db
from src.database.offer_url import fetch_gtin_url, fetch_google_shopping_url
from src.database.product import get_popular_product

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
	if production:
		# We do not have to decode since this function is triggered via
		# Firebase trigger and not PubSub where we need to decode.
		payload = event
		# Get the product_token which is the only key in the incoming dict
		product_token = payload['delta']['product_token'] # * generates list of keys
		# Decrypt the GTIN from the product_token
		# take the first GTIN if there are multiple one
		gtin = encryption.fernet_decrypt(product_token)
		if gtin:
			gtin = gtin.split(', ')[0]
			# query DB for associated URLs of this GTIN
			offer_urls = fetch_gtin_url(gtin)
			offer_urls = dict(offer_urls)
			# query DB for google shopping url
			gs_url = fetch_google_shopping_url(gtin)
			if gs_url:
				offer_urls['google_shopping_SE'] = gs_url
			# Enrich the data with the GTIN
			payload['delta']['gtin'] = gtin
			payload['delta']['offer_urls'] = offer_urls
			# Publish it to the topics which are consuming it
			publisher = Publisher('panprices', 'sherlock_products')
			publisher.publish_messages([payload['delta']])
			publisher_popular_products = Publisher('panprices', 'sherlock_popular_products')
			publisher_popular_products.publish_messages([payload['delta']])
			print("Trigger offer fetching for: " + gtin)
		else:
			print(f"Empty gtin encountered: {gtin}")

def live_search_offer_enricher(event, context, production=True) :
	"""
		Consumes messages on the topic 'live_search_offers', enriches them
		and then updates the Firebase Realtime Database with the output.
	"""
	try :
		payload = json.loads(base64.b64decode(event['data']))
		print(
			'Got', str(len(payload['offers'])), 'offers for gtin:',
			payload['gtin'],
			'from',
			payload['offer_source'],
			'with product_token:',
			payload['product_token']
		)
		# Fetch the service account key JSON file contents
		cred = credentials.Certificate('firebase_service_account.json')
		# Initialize the app with a service account, granting admin privileges
		app = firebase_admin.initialize_app(cred, {
			'databaseURL': 'https://panprices.firebaseio.com/'
		})
		# Open a connection to the database
		ref = db.reference('offers')
		# Choose the relevant search
		search_ref = ref.child(str(payload['product_token']))
		# Get the existing offers data, on this we need to calculate savings
		fetch_ref = search_ref.child('fetched_offers')
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
		# defind function for transaction
		# use transaction to avoid concurrency bug, because different data from
		# different fetching modules will come in at different message -> separate function invocation
		def enrich_data(existing_offers):
			if existing_offers != None and len(existing_offers) > 0:
				all_offers = existing_offers + payload['offers']
			else:
				all_offers = payload['offers']
			# Enrich and format all the combined offers
			if len(all_offers) > 0:
				return add_offers_metadata(all_offers)
			else:
				return []
		enriched_offers = fetch_ref.transaction(enrich_data)
		# Update the specific search in Firebase RTD with the newly fetched offers
		if production:
			search_ref.update({
				'fetched_offers/': enriched_offers,
				# 'fetched_sources/' + payload['offer_source']: True
			})
			print(f"Enriched {len(enriched_offers)} offers/{payload['gtin']}")
			# Publish all data to a separate topic for writing it down in batches to PSQL.
			publisher = Publisher('panprices', 'sherlock_live_offers')
			payload['offers'] = enriched_offers
			publisher.publish_messages([payload])
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
	start = time.time()
	# Print out the entire event object
	print('Publishing the following search for a product: ', str(event))
	# Publish the event to the sherlock_products Pubsub topic
	if production and ('results' not in event['delta'] or len(event['delta']['results']) == 0) and 'name' in event['delta'] and 'path_name' in event['delta']:
		try :
			end = time.time()
			event['delta']['performance'] = {
				'product_search_trigger': {
					'start': start * 1000,
					'end': end * 1000,
					'exeTime': (end - start) * 1000
				}
			}
			product_search_trigger_publisher = Publisher('panprices', 'sherlock_google_shopping')
			pub_results = product_search_trigger_publisher.publish_messages([event['delta']])
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
		# Create a timestamp for performance logging the whole pipeline
		start = time.time()
		payload = json.loads(base64.b64decode(event['data']))
		if 'search_query' not in payload:
			# this message come from retool update image app, skip it
			print("No search_query found, message come from retool, skip")
			return
		# Fetch the service account key JSON file contents
		cred = credentials.Certificate('firebase_service_account.json')
		# Initialize the app with a service account, granting admin privileges
		app = firebase_admin.initialize_app(cred, {
			'databaseURL': 'https://panprices.firebaseio.com/'
		})
		# Open a connection to the database
		ref = db.reference('product_search')
		# Choose the relevant search
		current_entry_ref = ref.child(str(payload['search_query']))
		# Get existing data in this specific search
		current_entry = current_entry_ref.get({"results"})[0]
		result = {}
		# If results exist add to existing results dict
		if current_entry != None and "results" in current_entry:
			result = current_entry["results"]
		# Generate an encrypted product token from the GTIN
		# take the first GTIN if there are multiple GTIN
		product_token = encryption.fernet_encrypt(
			payload["gtin"].split(', ')[0]
		)
		# Write all input data with the specific product_token as key
		result[product_token] = payload
		# Add the token to the data object as well
		result[product_token]["product_token"] = product_token
		# Remove some fields we don't want to present to the client
		del result[product_token]["product_url"]
		del result[product_token]["source"]
		del result[product_token]["img_encoded"]
		del result[product_token]["gtin"]
		# Only publish if we are running in production
		if production :
			# Update the specific search in Firebase RTD with the newly fetched offers
			if 'performance' in payload:
				end = time.time()
				result[product_token]['performance']['product_search_publish_result'] = {
					'start': start * 1000,
					'end': end * 1000,
					'exeTime': (end - start) * 1000,
					'delay_from_sherlock_upload_image': start * 1000 - payload['performance']['sherlock_upload_image']['end']
				}
			current_entry_ref.update({
				"results": result
			})
			# Store the specific product in another data path used for the client
			# to grab image and product_name in the offers page
			for prod_token in result :
				db.reference('products/' + prod_token).set(result[prod_token])
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
		ref = db.reference('product_search')
		# Choose the relevant search
		current_entry_ref = ref.child(str(payload['search_query']))
		# Only publish if we are running in production
		if production :
			# Update the specific search in Firebase RTD with the newly fetched offers
			current_entry_ref.update({
				"search_completed": True
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
		print("Starting to flush product_search path of data older then 1 hour.")
		flush_db.delete_data(
			bucket = 'product_search',
			hours_cutoff = 1,
			firebase_db = db
		)
		# Flush offers path of data older then 24 hour
		print("Starting to flush offers path of data older then 24 hour.")
		flush_db.delete_data(
			bucket = 'offers',
			hours_cutoff = 24,
			firebase_db = db
		)
	except Exception as e:
		print("There was an error: ", e)
		raise e
	finally :
		print("Delete job finished.")

def popular_product_search_trigger(event, context):
	product_tokens = get_popular_product()

	if len(product_tokens) <= 0:
		print("No popular product to fetch")
		return

	# Fetch the service account key JSON file contents
	cred = credentials.Certificate('firebase_service_account.json')
	# Initialize the app with a service account, granting admin privileges
	app = firebase_admin.initialize_app(cred, {
		'databaseURL': 'https://panprices.firebaseio.com/'
	})
	# Open a connection to the database
	ref = db.reference('offers')

	# transform a list of product token to the form we use in the firebase offers path
	# the result is in the form:
	# {
	# 	<product_token> : {
	# 		"offer_fetch_complete": False,
	#		"product_token": product_token,
	#		"created_at": int(round(time.time() * 1000)),
	#		"triggered_from_client": True,
	#		"popular": True ## this key is to signify the later step in the pipeline this offer is triggered from the popular product
	# 	}
	# }
	# convert to a list of tuple (key, value), before passing to dict()
	transformed_products = dict([(product_token, {
		"offer_fetch_complete": False,
		"product_token": product_token,
		"created_at": int(round(time.time() * 1000)),
		"triggered_from_client": True,
		"popular": True
	}) for product_token in product_tokens])

	ref.update(transformed_products)

	print(f"Trigger fetching offers for {len(product_tokens)} popular products")

def get_price_from_firebase(request) :
	request_json = request.get_json(silent=True)
	print(
		"The function was triggered with the following data: ",
		request_json
	)
	product_token = request_json.get('product_token')
	offer_id = request_json.get('offer_id')
	# return 400 if the body is incorrect
	if product_token is None or offer_id is None:
		return ("expecting product_token and offer_id", 400)

	# Fetch the service account key JSON file contents
	cred = credentials.Certificate('firebase_service_account.json')
	# Initialize the app with a service account, granting admin privileges
	app = firebase_admin.initialize_app(cred, {
		'databaseURL': 'https://panprices.firebaseio.com/'
	})
	# Open a connection to the database
	ref = db.reference('offers')
	# Choose the relevant search
	search_ref = ref.child(product_token)
	# Get the existing offers data, on this we need to calculate savings
	offers = search_ref.child('fetched_offers').get()
	for offer in offers :
		if offer.get('offer_id') == offer_id :
			return json.dumps(
				int(offer['price'])
			), 200, {'Content-Type': 'application/json'}
	return ("There wasn't any price on this offer", 400)
