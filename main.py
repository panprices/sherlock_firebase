import base64
import json
import firebase_admin
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
		except Exception as e :
			raise e
		print(pub_results)
		return pub_results

def live_search_offer_enricher(event, context, production=True) :
	"""
		Consumes messages on the topic 'live_search_offers', enriches them
		and then updates the Firebase Realtime Database with the output.
	"""
	payload = json.loads(base64.b64decode(event['data']))
	print('Got offers for search_id: ', payload['search_id'])
	# Fetch the service account key JSON file contents
	cred = credentials.Certificate('firebase_service_account.json')
	# Initialize the app with a service account, granting admin privileges
	app = firebase_admin.initialize_app(cred, {
		'databaseURL': 'https://panprices.firebaseio.com/'
	})
	# Open a connection to the database
	ref = db.reference('offerSearch')
	# Choose the relevant search
	search_ref = ref.child(str(payload['search_id']))
	# TODO: Enrich and format the data in this stage

	add_offers_metadata(payload['offers'])

	# Update the specific search in Firebase RTD with the newly fetched offers
	search_ref.update({
		'offers/' + payload['offer_source']: payload['offers']
	})
	# Kill the connection, otherwise the next instance trying to connect will crash
	firebase_admin.delete_app(app)

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
			publisher = Publisher('panprices', 'sherlock_product_search')
			pub_results = publisher.publish_messages([event['delta']])
		except Exception as e :
			raise e
		print(pub_results)
		return pub_results
