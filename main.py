import json
from src.pubsub.pubsub import Publisher
from src.helpers.helpers import format_search_offer_msg

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