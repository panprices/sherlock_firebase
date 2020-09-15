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
			'00190199383852': {
				'createdAt': '1598969652038',
				'gtin': '00190199383852',
				'price': 2500,
				'offerFetchComplete': False
			}
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
		"createdAt": 1600170673311,
		"gtin": "00190199389342",
		"offerFetchComplete": False,
		"triggeredFromClient": True,
		"offers": [
			{
				"product_id": None,
				"source": "kelkoo_NO",
				"retailer_product_name": "Apple iPhone 11 Pro 64GB Gold",
				"retailer_name": "Netonnet.no",
				"country": "NO",
				"price": "1119000",
				"currency": "NOK",
				"offer_url": "https://no-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1600170676023&.sig=NcKs4rAh6nFU.caSgKXIJ2I0zXQ-&affiliationId=96954748&catId=100020213&comId=6995723&contextLevel=1&contextOfferPosition=1&contextPageSize=2&country=no&ecs=ok&merchantid=6995723&offerId=5c22b0f698c293a251bb6ef143c33ff8&searchId=10769825031957_1600170676020_49164&searchQuery=&service=5&wait=true",
				"requested_at": "2020-09-15 11:51:16.038276",
				"match_score": 1
			},
			{
				"product_id": None,
				"source": "kelkoo_NO",
				"retailer_product_name": "Apple iPhone 11 Pro 64GB, gull",
				"retailer_name": "Telenor",
				"country": "NO",
				"price": "1159900",
				"currency": "NOK",
				"offer_url": "https://no-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1600170676024&.sig=pEW1slZQcu8CzZ2FZQWBNK7kPOg-&affiliationId=96954748&catId=100010713&comId=100480081&contextLevel=1&contextOfferPosition=2&contextPageSize=2&country=no&ecs=ok&merchantid=100480081&offerId=39b5c50b0a3ec3d7cd863454c41aae2a&searchId=10769825031957_1600170676020_49164&searchQuery=&service=5&wait=true",
				"requested_at": "2020-09-15 11:51:16.038302",
				"match_score": 1
			},
			{
				"product_id": None,
				"source": "kelkoo_SE",
				"retailer_product_name": "Apple iPhone 11 Pro 64GB Gold",
				"retailer_name": "Netonnet.se",
				"country": "SE",
				"price": "1099000",
				"currency": "SEK",
				"offer_url": "https://se-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1600170676077&.sig=bdbohhHS4r5cb8vv.vPOU1CST9M-&affiliationId=96954747&catId=100020213&comId=100456719&contextLevel=1&contextOfferPosition=1&contextPageSize=2&country=se&ecs=ok&merchantid=100456719&offerId=21234f9ca6e26be08322c9cc1f2ca3a8&searchId=10769920413306_1600170676071_29528&searchQuery=&service=5&wait=true",
				"requested_at": "2020-09-15 11:51:16.090122",
				"match_score": 1
			},
			{
				"product_id": None,
				"source": "kelkoo_SE",
				"retailer_product_name": "Apple iPhone 11 Pro 64GB - Guld",
				"retailer_name": "CyberPhoto",
				"country": "SE",
				"price": "1079000",
				"currency": "SEK",
				"offer_url": "https://se-go.kelkoogroup.net/ctl/go/sitesearchGo?.ts=1600170676077&.sig=C0vy7_XjF3PVhnXgtWfG3n0UZL0-&affiliationId=96954747&catId=100020213&comId=3375501&contextLevel=1&contextOfferPosition=2&contextPageSize=2&country=se&ecs=ok&merchantid=3375501&offerId=36e528db7971f8a97205753cc56d4aa7&searchId=10769920413306_1600170676071_29528&searchQuery=&service=5&wait=true",
				"requested_at": "2020-09-15 11:51:16.090150",
				"match_score": 1
			}
		],
		"offer_source": "kelkoo",
		"name": None,
		"product_id": None,
		"price": None
	}
	# Modify it to behave like the input in Cloud Functions
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
		"createdAt": 1598977013859,
		"name": "Sony Alpha A7",
		"pathName": "sony_alpha_a7",
		"searchCompleted": False,
		"source": "google_shopping",
		"product_name": "Sony A7 III Kamerahus ",
		"gtin": "04548736079656",
		"price": 2399000,
		"currency": "SEK",
		"img_encoded": "",
		"img_url": "https://storage.googleapis.com/panprices/products/c04357ec-aa55-3579-bbc6-0265983a5e78.jpg",
		"country": "SE",
		"product_url": "https://www.google.com/shopping/product/17082225217934858373",
		"product_description": "Sony A7 Mark III systemkameranar  utrustad med flera av A9 och A7R lll kamerornas specialfunktioner. Jämfört med A7 ll kameran har A7 lll modellen bl.a. en ny bakbelyst 24.2MP:s Exmor R CMOS sensor och ny Bionz X –prosessor. 693 autofokuspunkter, 10fps AF/AE seriebildstagning, 4K-videofilmning samt ett batteri med högre kapacitet. Fantastisk bildkvalitet samt suverän färgåtergivning har varit undantag. Lagring av  svåra vinklar.",
		"searchQuery": "sony_alpha_a7",
		"bundleResult": False
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
