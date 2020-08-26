import argparse
import json
import base64

from main import offer_search_trigger
from main import live_search_offer_enricher
from main import product_search_trigger
from main import product_search_publish_result
from main import delete_old_firebase_data
from main import sherlock_shopping_finish_signal

def demo_offer_search_trigger() :
	# Mock a message
	message = {
		"data": None,
		"delta": {
			'name': 'Bose QuietComfort 35 II',
			'id': '1357530',
			'price': 2500,
			'search_id': '12312',
			'ean': '00017817770613',
			'user_agent': 'blabla'
		}
	}
	# Define a mocked context
	context = {
		'event_id': '-1',
		'resource': 'projects/_/instances/panprices/refs/offerSearch/'
	}
	# Execute the function
	result = offer_search_trigger(message, context, production=False)
	print(result)

def demo_live_search_offer_enricher() :
	# Mock a message
	message = {
		"offers": [
			{
				"product_id": "1357530",
				"source": "kelkoo_DE",
				"retailer_product_name": "Bose L1 Model II/B2",
				"retailer_name": "Thomann",
				"country": "DE",
				"price": "233200",
				"currency": "EUR",
				"offer_url": "https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1583176995007&.sig=s9TqnXFk0LTl2hK.9W12KTgJAGQ-&affiliationId=96954745&catId=122301&comId=3936523&contextLevel=1&contextOfferPosition=1&contextPageSize=13&country=de&ecs=ok&merchantid=3936523&offerId=4e8f43c7dd091f29bb354a50f266852d&searchId=10769920718718_1583176994996_236865&searchQuery=Ym9zZSBxdWlldGNvbWZvcnQgMzUgaWk%3D&service=5&wait=true",
				"requested_at": "2020-03-02 19:23:15.031627",
				"match_score": 1
			},
			{
				"product_id": "1357530",
				"source": "kelkoo_DE",
				"retailer_product_name": "Bose L1 Model II/B1",
				"retailer_name": "Thomann",
				"country": "DE",
				"price": "217000",
				"currency": "EUR",
				"offer_url": "https://de-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1583176995007&.sig=OUitcyJEswHLIOx5rYsZNGRFFTo-&affiliationId=96954745&catId=122301&comId=3936523&contextLevel=1&contextOfferPosition=2&contextPageSize=13&country=de&ecs=ok&merchantid=3936523&offerId=b1e44900a6fa014a95126812903ac948&searchId=10769920718718_1583176994996_236865&searchQuery=Ym9zZSBxdWlldGNvbWZvcnQgMzUgaWk%3D&service=5&wait=true",
				"requested_at": "2020-03-02 19:23:15.031687",
				"match_score": 1
			}
		],
		"offer_source": "kelkoo",
		"name": "Bose QuietComfort 35 II",
		"product_id": "1357530",
		"price": 2500,
		"search_id": 2848,
		"ean": "00017817770613"
	}
	data = {
		'data': base64.b64encode(json.dumps(message).encode())
	}
	# Define a mocked context
	context = {
		'event_id': '-1'
	}
	# Execute the function
	result = live_search_offer_enricher(data, context, production=False)
	print(result)

def demo_product_search_trigger() :
	# Mock a message
	message = {
		"data": None,
		"delta": {
			"Google Home": {
				"createdAt": 1597938406487,
				"updatedAt": 1597938454902,
				"results": []
			},
		}
	}
	# Define a mocked context
	context = {
		'event_id': '-1',
		'resource': 'projects/_/instances/panprices/refs/productSearch/'
	}
	# Execute the function
	result = product_search_trigger(message, context, production=False)
	print(result)

def demo_product_search_publish_result() :
	# Mock a message
	message = {
		"source": "google_shopping",
		"product_name": "Apple iPhone XR - 64GB - GSM Unlocked - Black ",
		"gtin": "00753359751696",
		"price": 59900,
		"currency": "USD",
		"img_encoded": "",
		"img_url": "https://storage.googleapis.com/panprices/products/884a2431-8af1-3fe3-94f8-f57f7e099033.jpg",
		"country": "NULL",
		"product_url": "https://www.google.com/shopping/product/161535565509281091",
		"product_description": "An advanced LCD. Even faster Face ID. And a breakthrough camera system. The iPhone XR is beautiful any way you look at it.",
		"searchQuery": "apple_iphone_10"
	}
	data = {
		'data': base64.b64encode(json.dumps(message).encode())
	}
	# Define a mocked context
	context = {
		'event_id': '-1'
	}
	# Execute the function
	result = product_search_publish_result(data, context, production=False)
	print(result)

def demo_sherlock_shopping_finish_signal() :
	# Mock a message
	message = {
		"searchQuery": "foo_bar"
	}
	data = {
		'data': base64.b64encode(json.dumps(message).encode())
	}
	# Define a mocked context
	context = {
		'event_id': '-1'
	}
	# Execute the function
	result = sherlock_shopping_finish_signal(data, context , production=False)
	print(result)

def demo_delete_old_firebase_data() :
	# Mock a message
	message = {
		"hello": "hi"
	}
	data = {
		'data': base64.b64encode(json.dumps(message).encode())
	}
	# Define a mocked context
	context = {
		'event_id': '-1'
	}
	# Execute the function
	result = delete_old_firebase_data(data, context)
	print(result)

if __name__ == '__main__' :
	# Instantiate the parser
	parser = argparse.ArgumentParser(
		formatter_class=argparse.RawDescriptionHelpFormatter,
		epilog="Argument Parser for the demo script.")
	# Define the arguments
	parser.add_argument(
		'-ost',
		'--offer_search_trigger',
		action='store_true',
		help='Listen to Realtime Firebase Triggers and Publish to Pubsub.'
	)
	parser.add_argument(
		'-oe',
		'--offer_enricher',
		action='store_true',
		help='Enrich offer output and update Firebase Realtime Database.'
	)
	parser.add_argument(
		'-pst',
		'--product_search_trigger',
		action='store_true',
		help='Listen to Realtime Firebase Triggers on productSearch and trigger to PubSub.'
	)
	parser.add_argument(
		'-pspr',
		'--product_search_publish_result',
		action='store_true',
		help='Publish to Realtime Firebase on productSearch.'
	)
	parser.add_argument(
		'-ssfs',
		'--sherlock_shopping_finish_signal',
		action='store_true',
		help='Consume finished message from Sherlock Google Shopping and update Firebase.'
	)
	parser.add_argument(
		'-dofd',
		'--delete_old_firebase_data',
		action='store_true',
		help='Publish to Realtime Firebase on productSearch.'
	)
	# Parse the args
	args = parser.parse_args()
	# Decide on execution
	if args.offer_search_trigger :
		demo_offer_search_trigger()
	elif args.offer_enricher :
		demo_live_search_offer_enricher()
	elif args.product_search_trigger :
		demo_product_search_trigger()
	elif args.product_search_publish_result:
		demo_product_search_publish_result()
	elif args.delete_old_firebase_data:
		demo_delete_old_firebase_data()
	elif args.sherlock_shopping_finish_signal:
		demo_sherlock_shopping_finish_signal()
