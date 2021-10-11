import argparse
import json
import base64
import glob

from main import offer_search_trigger, store_finished_offers
from main import live_search_offer_enricher
from main import delete_old_firebase_data
from main import sherlock_shopping_finish_signal
from main import popular_product_search_trigger
from main import get_price_from_firebase
from main import create_offer_firebase
from main import create_product_search_firebase


def demo_offer_search_trigger():
    # Mock a message
    product_token = "gAAAAABf3D0m2el6n0TB6D5fA-cNWVRCz_HEffoBPkQlb5oP2EU2_7AbWUCwj2145CVkW0C9No1DfWkRuLK5K8PLLu23J8UnFw=="
    message = {
        "data": None,
        "delta": {
            "created_at": 1623244478117,
            "offerFetchComplete": False,
            "product_token": product_token,
            "triggered_from_client": True,
        },
    }

    class Context:
        def __init__(self) -> None:
            self.event_id = "-1"
            self.resource = (
                f"projects/_/instances/panprices/refs/offers/SE/{product_token}"
            )

    context = Context()
    # Execute the function
    result = offer_search_trigger(
        # We do not encode this to byte since Firebase input value
        # from trigger is different then that from PubSub.
        message,
        context,
        production=True,
    )


def demo_live_search_offer_enricher():
    for filepath in glob.iglob("demo_data/live_search_offer_*.json"):
        with open(filepath) as json_offer:
            # Modify it to behave like the input in Cloud Functions
            data = {
                "data": base64.b64encode(json.dumps(json.load(json_offer)).encode())
            }
            # Define a mocked context
            context = {"event_id": "-1"}
            # Execute the function
            live_search_offer_enricher(data, context, production=False)


def demo_sherlock_shopping_finish_signal():
    # Mock a message
    message_client = {
        "search_query": "foo_bar",
        "triggered_by": {"source": "client"},
        "result_size": 3,
    }
    data = {"data": base64.b64encode(json.dumps(message_client).encode())}
    # Define a mocked context
    context = {"event_id": "-1"}
    # Execute the function
    result = sherlock_shopping_finish_signal(data, context, production=False)
    print("Client result:")
    print(result)

    message_batch = {
        "search_query": "foo_bar",
        "triggered_by": {
            "source": "batch",
            "batch_id": "demo_batch_id____sherlock_finish_signal",
        },
        "result_size": 35,
    }
    data = {"data": base64.b64encode(json.dumps(message_batch).encode())}
    context = {"event_id": "-1"}
    result = sherlock_shopping_finish_signal(data, context, production=False)
    print("Batch result:")
    print(result)


def demo_delete_old_firebase_data():
    # Mock a message
    message = {"hello": "hi"}
    data = {"data": base64.b64encode(json.dumps(message).encode())}
    # Define a mocked context
    context = {"event_id": "-1"}
    # Execute the function
    result = delete_old_firebase_data(data, context)
    print(result)


def demo_popular_product_search_trigger():
    popular_product_search_trigger({}, {})


def demo_get_price_from_firebase():
    class Request:
        def get_json(self, silent=False):
            return {
                "product_token": "gAAAAABfdHhqiYnhuHLsD4V-O3q2hc7NnPrvOVf92OP5fpUnrNwraQzPgS3hMI5KLBFD0LR5JT9py5IZ4b2VZ5UQJRJQypJPdw==",
                "offer_id": "0e9930d3-39dc-47bc-b91e-be9ecf57aa20",
            }

        headers = {"Panprices-User-Country": "SE"}

    request = Request()
    result = get_price_from_firebase(request)
    print(result)


def demo_create_offer_firebase():
    product_token = "gAAAAABf6hhzN9p1LcmGNtmLilI_Olq1g98g-o27p_7O_cA6KFqN8n3RbhlNRcB_mH3LAGNJK8ufoguy37aSqakO8Ki_1r8inQ=="

    class Request:
        method = ""

        def get_json(self, silent=False):
            return {
                "product_token": product_token,
                "user_country": "SE",
            }

    print(f"Trying to create a new offer at /offers/SE/{product_token}")
    request = Request()
    response = create_offer_firebase(request)
    print(response)


def demo_create_product_search_firebase():
    class Request:
        def method(self):
            return ""

        def get_json(self, silent=False):
            return {"cleaned_query": "filco_keyboard", "query": "Filco Keyboard"}

    print(
        f"Trying to create a new product search at /product_search/test_product_search"
    )
    request = Request()

    response = create_product_search_firebase(request)
    print(response)


def demo_store_finished_offers():
    # Mock a message
    message = {
        "product_token": "gAAAAABgJGp1h1n-UQuexdHCMzmnmr40NauwhV9RRjWl7NFqPy-0aUuufCdG2WCHGSsnza1TdzJp4xfoxnCb4ikC5LaESIiFWA==",
        "gtin": "00194276341971",
        "user_country": "SE",
    }
    data = {"data": base64.b64encode(json.dumps(message).encode())}
    # Define a mocked context
    context = {"event_id": "-1"}
    # Execute the function
    result = store_finished_offers(data, context)
    print(result)


if __name__ == "__main__":
    # Instantiate the parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Argument Parser for the demo script.",
    )
    # Define the arguments
    parser.add_argument(
        "-ost",
        "--offer_search_trigger",
        action="store_true",
        help="Listen to Realtime Firebase Triggers and Publish to Pubsub.",
    )
    parser.add_argument(
        "-oe",
        "--offer_enricher",
        action="store_true",
        help="Enrich offer output and update Firebase Realtime Database.",
    )
    parser.add_argument(
        "-ssfs",
        "--sherlock_shopping_finish_signal",
        action="store_true",
        help="Consume finished message from Sherlock Google Shopping and update Firebase.",
    )
    parser.add_argument(
        "-dofd",
        "--delete_old_firebase_data",
        action="store_true",
        help="Publish to Realtime Firebase on productSearch.",
    )
    parser.add_argument(
        "-ppst",
        "--popular_product_search_trigger",
        action="store_true",
        help="fetch product with non null popularity index and publish to firebase offers path",
    )
    parser.add_argument(
        "-gpff",
        "--get_price_from_firebase",
        action="store_true",
        help="Return the price from Firebase with a product token and offer id",
    )
    parser.add_argument(
        "-cof",
        "--create_offer_firebase",
        action="store_true",
        help="Create a test offer in at /offers/<product_token>",
    )
    parser.add_argument(
        "-cpsf",
        "--create_product_search_firebase",
        action="store_true",
        help="Create a test product_search at /offers/<cleaned_query>",
    )
    parser.add_argument(
        "-sfo",
        "--store_finished_offers",
        action="store_true",
        help="Store an offer search in big query",
    )
    # Parse the args
    args = parser.parse_args()
    # Decide on execution
    if args.offer_search_trigger:
        demo_offer_search_trigger()
    elif args.offer_enricher:
        demo_live_search_offer_enricher()
    elif args.delete_old_firebase_data:
        demo_delete_old_firebase_data()
    elif args.sherlock_shopping_finish_signal:
        demo_sherlock_shopping_finish_signal()
    elif args.popular_product_search_trigger:
        demo_popular_product_search_trigger()
    elif args.get_price_from_firebase:
        demo_get_price_from_firebase()
    elif args.create_offer_firebase:
        demo_create_offer_firebase()
    elif args.create_product_search_firebase:
        demo_create_product_search_firebase()
    elif args.store_finished_offers:
        demo_store_finished_offers()
