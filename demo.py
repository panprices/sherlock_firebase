import argparse
import json
import base64

from main import offer_search_trigger
from main import live_search_offer_enricher
from main import product_search_trigger
from main import product_search_publish_result
from main import delete_old_firebase_data
from main import sherlock_shopping_finish_signal
from main import popular_product_search_trigger

def demo_offer_search_trigger() :
	# Mock a message
	message = {
	  "data": None,
	  "delta": {
	    "offer_fetch_complete": False,
	    "product_token": "gAAAAABfkqs-sXrHEkUNLW6-jK7tM2o7QZzg1NsJqywqxnpREDNCV-ZO3DeXsMffG4siqL6S6HwhO9nZeMRXaGdynG3UxlqbzA==",
	    "created_at": 1600961105031,
	    "triggered_from_client": True
	  }
	}
	# Define a mocked context
	context = {
		'event_id': '-1',
		'resource': 'projects/_/instances/panprices/refs/offerSearch/'
	}
	# Execute the function
	result = offer_search_trigger(
		# We do not encode this to byte since Firebase input value
		# from trigger is different then that from PubSub.
		message,
		context,
		production=False
	)
	print(result)

def demo_live_search_offer_enricher() :
	# Mock two messages
	messages = [
		{
		    "created_at":1604323264073,
		    "gtin":"03616131072020",
		    "offer_fetch_complete":False,
		    "offer_urls":{
		        "geizhals_DE":"https://geizhals.de/1898461540",
		        "google_shopping_SE":"https://www.google.com/shopping/product/10166805956347853825",
		        "pricerunner_DK":None,
		        "pricerunner_SE":None,
		        "pricerunner_UK":None,
		        "prisjakt_SE":None
		    },
		    "product_token":"gAAAAABfn-KcweQ0JV0a0Husf-SXT3aihO4X-v6MogqtBhCzuaRNsb9ERKPvAMYTo22KJOnERl9leC5L_kK4ITtacmXKofB3wQ==",
		    "triggered_from_client":True,
		    "offers":[
		        {
		            "offer_source":"google_shopping_SE",
		            "retailer_name":"GUCCI",
		            "offer_url":"/aclk?sa=L&ai=DChcSEwjk_PL0--PsAhVICIgJHa8aC-sYABAGGgJxbg&sig=AOD64_1Vz5b3cmOsSJJlKl29zyVl0M4zsA&ctype=5&q=&ved=0ahUKEwiz_PD0--PsAhWYoXIEHUhBCekQ2ikIEw&adurl=",
		            "country":"SE",
		            "retail_prod_name":"Gucci Axelremväska Dam Svart Tröja ONESIZE",
		            "price":"2174990",
		            "currency":"SEK",
		            "requested_at":"2020-11-02T13:30:06Z",
		            "match_score":None
		        },
		        {
		            "offer_source":"google_shopping_SE",
		            "retailer_name":"farfetch.com",
		            "offer_url":"/aclk?sa=L&ai=DChcSEwjk_PL0--PsAhVICIgJHa8aC-sYABAEGgJxbg&sig=AOD64_3V-myZeuudAeiObIEQz_aCpUk92w&ctype=5&q=&ved=0ahUKEwiz_PD0--PsAhWYoXIEHUhBCekQ2ikIFw&adurl=",
		            "country":"SE",
		            "retail_prod_name":"Gucci Axelremväska Dam Svart Tröja ONESIZE",
		            "price":"2185800",
		            "currency":"SEK",
		            "requested_at":"2020-11-02T13:30:06Z",
		            "match_score":None
		        }
		    ],
		    "offer_source":"google_shopping_SE"
		},
		{
		    "created_at":1604323264073,
		    "gtin":"03616131072020",
		    "offer_fetch_complete":False,
		    "offer_urls":{
		        "geizhals_DE":"https://geizhals.de/1898461540",
		        "google_shopping_SE":"https://www.google.com/shopping/product/10166805956347853825",
		        "pricerunner_DK":None,
		        "pricerunner_SE":None,
		        "pricerunner_UK":None,
		        "prisjakt_SE":None
		    },
		    "product_token":"gAAAAABfn-KcweQ0JV0a0Husf-SXT3aihO4X-v6MogqtBhCzuaRNsb9ERKPvAMYTo22KJOnERl9leC5L_kK4ITtacmXKofB3wQ==",
		    "triggered_from_client":True,
		    "offers":[
		        {
		            "offer_source":"google_shopping_SE",
		            "retailer_name":"Mytheresa Sweden",
		            "offer_url":"/aclk?sa=L&ai=DChcSEwjk_PL0--PsAhVICIgJHa8aC-sYABAHGgJxbg&sig=AOD64_28ELgD21tpk59rVqmMfCjLUpJt7A&ctype=5&q=&ved=0ahUKEwiz_PD0--PsAhWYoXIEHUhBCekQ2ikIGg&adurl=",
		            "country":"SE",
		            "retail_prod_name":"Gucci Axelremväska Dam Svart Tröja ONESIZE",
		            "price":"2174990",
		            "currency":"SEK",
		            "requested_at":"2020-11-02T13:30:06Z",
		            "match_score":None
		        },
		        {
		            "offer_source":"google_shopping_SE",
		            "retailer_name":"miinto.se",
		            "offer_url":"/aclk?sa=L&ai=DChcSEwjk_PL0--PsAhVICIgJHa8aC-sYABAFGgJxbg&sig=AOD64_2k-QyMYPIfpa8KDXyEGsRjIrkfGA&ctype=5&q=&ved=0ahUKEwiz_PD0--PsAhWYoXIEHUhBCekQ2ikIHw&adurl=",
		            "country":"SE",
		            "retail_prod_name":"Gucci Axelremväska Dam Svart Tröja ONESIZE",
		            "price":"2200800",
		            "currency":"SEK",
		            "requested_at":"2020-11-02T13:30:06Z",
		            "match_score":None
		        }
		    ],
		    "offer_source":"google_shopping_SE"
		}
	]

	for message in messages :
		# Modify it to behave like the input in Cloud Functions
		data = {
			'data': base64.b64encode(json.dumps(message).encode())
		}
		# Define a mocked context
		context = {
			'event_id': '-1'
		}
		# Execute the function
		live_search_offer_enricher(data, context, production=False)

def demo_product_search_trigger() :
	# Mock a message
	message = {
		"data": None,
		"delta": {
			"Google Home": {
				"created_at": 1597938406487,
				"updated_at": 1597938454902,
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
		"created_at": 1598977013859,
		"name": "Sony Alpha A7",
		"path_name": "sony_alpha_a7",
		"search_completed": False,
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
		"search_query": "sony_alpha_a7",
		"bundle_result": False,
		"performance":{
			"product_search_trigger":{
				"start":1600779213989.5525,
				"end":1600779214358.9958,
				"exeTime":369.443416595459
			},
			"sherlock_google_shopping":{
				"start":1600779779701.9631,
				"end":1600779782203.0269,
				"exeTime":2501.063585281372,
				"delay_from_product_search_trigger":647646.1186523438
			},
			"sherlock_upload_image":{
				"start":1600780764941.1404,
				"end":1600780765311.0593,
				"exeTime":369.9188232421875,
				"delay_from_sherlock_google_shopping":982738.1135253906
			}
		}
	}
	data = {
		'data': base64.b64encode(json.dumps(message).encode())
	}
	# Define a mocked context
	context = {
		'event_id': '-1'
	}
	# Execute the function
	result = product_search_publish_result(
		data,
		context,
		production=True
	)
	print(result)

def demo_sherlock_shopping_finish_signal() :
	# Mock a message
	message = {
		"search_query": "foo_bar"
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

def demo_popular_product_search_trigger():
	popular_product_search_trigger({}, {})

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
	parser.add_argument(
		'-ppst',
		'--popular_product_search_trigger',
		action='store_true',
		help='fetch product with non null popularity index and publish to firebase offers path'
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
	elif args.popular_product_search_trigger:
		demo_popular_product_search_trigger()
