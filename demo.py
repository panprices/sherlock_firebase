import argparse
import json
import base64

from main import offer_search_trigger

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
	# Parse the args
	args = parser.parse_args()
	# Decide on execution
	if args.offer_search_trigger :
		demo_offer_search_trigger()
