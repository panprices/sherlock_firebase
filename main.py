import base64
import json
from src.store_offers.best_offers_db import get_best_offer, store_best_offer_in_db
from src.store_offers.bigquery import store_offers_in_bq
import firebase_admin
import time
import logging
from firebase_admin import credentials
from firebase_admin import db
from google.cloud import bigquery


import src.helpers.encryption as encryption
from src.pubsub.pubsub import Publisher
from src.helpers.helpers import get_user_country_from_fb_context
from src.helpers import helpers
from src.enricher.enricher import add_offers_metadata
from src.enricher.sources_are_done import mark_source_as_done
from src.firebase import flush_db
from src.database.offer_url import fetch_gtin_url, fetch_google_shopping_url
from src.database.product import get_popular_products

logging.basicConfig(level=logging.INFO)


def _initialize_firebase():
    # Fetch the service account key JSON file contents
    cred = credentials.Certificate("firebase_service_account.json")
    # Initialize the app with a service account, granting admin privileges
    app = firebase_admin.initialize_app(
        cred, {"databaseURL": "https://panprices.firebaseio.com/"}
    )
    return app


# Global (instance-wide) scope, which runs at instance cold-start.
app = _initialize_firebase()


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
    print("Publishing the following live search for product: ", str(event))

    # There are some few edge cases where a property is updated just
    # after the offer search object is deleted. That triggers this function.
    if event["delta"].get("product_token") is None:
        # Delete the "broken" entry
        db.reference(context.resource).delete()
        return

    # Publish the event to the sherlock_products Pubsub topic
    if production:
        # We do not have to decode since this function is triggered via
        # Firebase trigger and not PubSub where we need to decode.
        payload = event
        # Get the product_token which is the only key in the incoming dict
        product_token = payload["delta"]["product_token"]
        # Decrypt the GTIN from the product_token
        # take the first GTIN if there are multiple one
        gtin = encryption.fernet_decrypt(product_token)
        if gtin:
            gtin = gtin.split(", ")[0]
            # query DB for associated URLs of this GTIN
            offer_urls = fetch_gtin_url(gtin)
            offer_urls = dict(offer_urls)
            # query DB for google shopping url
            gs_url = fetch_google_shopping_url(gtin)
            if gs_url:
                offer_urls["google_shopping_SE"] = gs_url
            # Enrich the data with the GTIN
            payload["delta"]["gtin"] = gtin
            payload["delta"]["offer_urls"] = offer_urls

            # Enrich the data with user_country
            user_country = get_user_country_from_fb_context(context)
            print(f"user_country detected: {user_country}")
            payload["delta"]["user_country"] = user_country

            # Publish it to the topics which are consuming it
            publisher = Publisher("panprices", "sherlock_products")
            publisher.publish_messages([payload["delta"]])
            publisher_popular_products = Publisher(
                "panprices", "sherlock_popular_products"
            )
            publisher_popular_products.publish_messages([payload["delta"]])
            print(
                f"Trigger offer fetching for gtin {gtin}, published message: {payload['delta']}"
            )
        else:
            print(f"Empty gtin encountered: {gtin}")


def live_search_offer_enricher(event, context, production=True):
    """
    Consumes messages on the topic 'live_search_offers', enriches them
    and then updates the Firebase Realtime Database with the output.
    """
    try:
        payload = json.loads(base64.b64decode(event["data"]))
        print(
            "Got",
            str(len(payload["offers"])),
            "offers for gtin:",
            payload["gtin"],
            "from",
            payload["offer_source"],
            "with product_token:",
            payload["product_token"],
        )

        # Open a connection to the database
        if "user_country" in payload.keys():
            print(f"Key user_country is {payload['user_country']}")
        else:
            print("Key user_country is not specified in payload. Defaulting to 'SE'.")
        user_country = payload.get("user_country", "SE")
        ref = db.reference(f"offers/{user_country}")
        # Choose the relevant search
        search_ref = ref.child(str(payload["product_token"]))
        # Get the existing offers data, on this we need to calculate savings
        fetch_ref = search_ref.child("fetched_offers")
        # Join existing and new offers together to a list (if existing data exists)
        """
            CASES TO CHECK FOR HERE:
             - If there are no offers Firebase will return this weird tuple:
             (None, 'null_etag') This happens in test.
             - If the client has created a message and this function gets triggered
             in production we will return something like {createdAt:X, gtin:Y, offers:Z}
             but the key fetchedOffers won't be in there because that property gets
             set by this function the first time.
        """
        # defind function for transaction
        # use transaction to avoid concurrency bug, because different data from
        # different fetching modules will come in at different message -> separate function invocation
        def enrich_data(existing_offers):
            if existing_offers != None and len(existing_offers) > 0:
                all_offers = existing_offers + payload["offers"]
            else:
                all_offers = payload["offers"]
            # Enrich and format all the combined offers
            if len(all_offers) > 0:
                # metadata from PSQL
                return add_offers_metadata(all_offers, user_country)
            else:
                return []

        enriched_offers = fetch_ref.transaction(enrich_data)
        print(
            f"Enriched {len(enriched_offers)} offers/{user_country}/{payload['gtin']}"
        )

        all_sources_done = mark_source_as_done(
            user_country, str(payload["product_token"]), payload["offer_source"]
        )

        if all_sources_done:
            print(f"Offer fetch for token {str(payload['product_token'])} is complete")
            search_ref.child("offer_fetch_complete").set(True)

            search_complete_payload = {
                "product_token": str(payload["product_token"]),
                "gtin": payload["gtin"],
                "user_country": user_country,
            }

            search_complete_publisher = Publisher("panprices", "offer_search_complete")
            search_complete_publisher.publish_messages([search_complete_payload])

        if production:
            # Publish all data to a separate topic for writing it down in batches to PSQL.
            publisher = Publisher("panprices", "sherlock_live_offers")
            payload["offers"] = enriched_offers
            publisher.publish_messages([payload])
    except Exception as e:
        msg_string = json.dumps(payload)
        print(f"something went wrong when handling message: {msg_string}")
        raise e


def sherlock_shopping_finish_signal(event, context, production=True):
    """
    Listens to changes on pubsub finish topic from google shopping
    and updates the firebase object for the specific search query.
    This way the client will know when to render a message saying
    that the search never resulted in any valid matches with GTIN
    which is important for the user experience.
    """
    # Only publish if we are running in production
    if production:
        payload = json.loads(base64.b64decode(event["data"]))
        triggered_by = payload["triggered_by"]
        search_query = str(payload["search_query"])
        failed = payload.get("failed", False)
        result_size = payload.get("result_size")

        # Update the specific search in Firebase RTD with the newly fetched offers
        db.reference("product_search").child(search_query).update(
            {"search_completed": True}
        )

        if triggered_by.get("source") == "batch":
            batch_id = triggered_by["batch_id"]

            if failed:
                (
                    db.reference("batch_search")
                    .child(batch_id)
                    .child("products")
                    .child(search_query)
                    .update(
                        {
                            "expected_result_size": 0,
                            "sherlock_product_search_done": True,
                        }
                    )
                )
            else:
                (
                    db.reference("batch_search")
                    .child(batch_id)
                    .child("products")
                    .child(search_query)
                    .update(
                        {
                            "expected_result_size": result_size,
                            "sherlock_product_search_done": True,
                        }
                    )
                )


def delete_old_firebase_data(event, context):
    country_codes = db.reference("offers").get(shallow=True)
    country_codes = [c for c in country_codes]

    try:
        print("Starting to flush product_search path of data older then 1 hour.")
        flush_db.delete_data(bucket="product_search", hours_cutoff=1, firebase_db=db)
        for country in country_codes:
            # Flush offers path of data older then 24 hour
            print(
                f"Starting to flush offers path of data older then 24 hour for {country}."
            )
            flush_db.delete_data(
                bucket=f"offers/{country}", hours_cutoff=24, firebase_db=db
            )
    except Exception as e:
        print("There was an error: ", e)
        raise e
    finally:
        print("Delete job finished.")


def popular_product_search_trigger(event, context):
    """Trigger live_search on popular products to keep them up-to-date."""
    product_tokens = get_popular_products()

    if len(product_tokens) <= 0:
        print("No popular product to fetch")
        return

    # transform a list of product token to the form we use in the firebase offers path
    # the result is in the form:
    # {
    # 	<product_token> : {
    # 		"offer_fetch_complete": False,
    # 		"product_token": product_token,
    # 		"created_at": int(round(time.time() * 1000)),
    # 		"triggered_from_client": True,
    # 		"popular": True ## this key is to signify the later step in the pipeline this offer is triggered from the popular product
    # 	}
    # }

    # Here we update the products by deleting the offers before re-create them
    # in order to trigger offer fetching.

    country_codes = db.reference("offers").get(shallow=True)
    country_codes = [c for c in country_codes]

    # Delete in bulk by updating them with empty data:
    products = {}
    for product_token in product_tokens:
        products[product_token] = None

    for country in country_codes:
        db.reference(f"offers/{country}").update(products)

        if len(products) > 0:
            db.reference(f"offers/{country}").update(products)

    # Recreate the products:
    for product_token in product_tokens:
        products[product_token] = {
            "offer_fetch_complete": False,
            "product_token": product_token,
            "created_at": int(round(time.time() * 1000)),  # ms since epoch
            "triggered_from_client": True,
            "popular": True,
        }

    for country in country_codes:
        db.reference(f"offers/{country}").update(products)

    print(f"Trigger fetching offers for {len(product_tokens)} popular products")


def get_price_from_firebase(request):
    user_country = request.headers.get("Panprices-User-Country")
    if user_country is None:
        return ("expecting header Panprices-User-Country", 400)

    if user_country != "SE" and user_country != "FI":
        return ("Panprices-User-Country has to be 'SE' or 'FI'", 400)

    request_json = request.get_json(silent=True)
    print("The function was triggered with the following data: ", request_json)
    product_token = request_json.get("product_token")
    offer_id = request_json.get("offer_id")
    # return 400 if the body is incorrect
    if product_token is None or offer_id is None:
        return ("expecting product_token and offer_id", 400)

    # Open a connection to the database
    ref = db.reference(f"offers/{user_country}")
    # Choose the relevant search
    search_ref = ref.child(product_token)
    # Get the existing offers data, on this we need to calculate savings
    offers = search_ref.child("fetched_offers").get()

    if offers is None:
        return ("There wasn't any price on this offer", 400)

    for offer in offers:
        # Get the specific offer and verify that this offer is available
        # for direct check out.
        if offer.get("offer_id") == offer_id and offer.get("direct_checkout") is True:
            # Grab the price from the object
            return (
                json.dumps(int(offer["direct_checkout_price"])),
                200,
                {"Content-Type": "application/json"},
            )
    return ("There wasn't any price on this offer", 400)


def create_offer_firebase(request):
    """Create a new offer object at /offers/<country_code>/<product_token>.

    The request body should be in JSON format and contain a `product_token` field.
    """
    if request.method == "OPTIONS":
        # Allows POST requests from any origin with the Content-Type
        # header and caches preflight response for an hour (3600s).
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            # 'Access-Control-Allow-Headers': 'Content-Type',
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "3600",
        }
        return ("", 204, headers)

    body = request.get_json()
    if body is None:
        msg = "Received an empty body request."
        logging.error(msg)
        return msg, 400
    if "product_token" not in body:
        msg = "The product_token field was not provided."
        logging.error(msg)
        return msg, 400
    if "user_country" not in body:
        msg = "The user_country field was not provided."
        logging.error(msg)
        return msg, 400

    user_country = body.get("user_country", "SE")
    if user_country not in ["SE", "FI"]:
        msg = f"The user_country field has to be SE or FI. It was: '{user_country}'"
        logging.error(msg)
        return msg, 400

    product_token = body["product_token"]

    offer_sources = helpers.get_offer_sources()
    offer = {
        "product_token": product_token,
        "product_id": body.get("product_id"),
        "created_at": int(time.time() * 1000),  # ms since epoch
        "triggered_from_client": True,
        "offer_fetch_complete": False,
        "offer_sources_done": {source: False for source in offer_sources},
    }
    try:
        db.reference(f"offers/{user_country}").child(str(product_token)).delete()
        db.reference(f"offers/{user_country}").child(str(product_token)).set(offer)
    except TypeError as ex:
        logging.error(ex)
        return "The request body is not serializable.", 400
    except db.exceptions.FirebaseError as ex:
        logging.error(ex)
        return "Error when communicating with Firebase server.", 500
    except Exception as ex:
        error_message = "Unexpected error: " + str(ex)
        logging.error(error_message)
        return error_message, 400

    # Set CORS headers for the main request
    response_headers = {"Access-Control-Allow-Origin": "*"}
    return (
        json.dumps({"success": True}),
        200,
        response_headers,
    )


def create_product_search_firebase(request):
    """Create a new product search object at /product_search/<cleaned_query>.

    The request body should be in JSON format and contain a `query` and a `cleaned_query` field.
    """
    if request.method == "OPTIONS":
        # Allows POST requests from any origin with the Content-Type
        # header and caches preflight response for an hour (3600s).
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            # 'Access-Control-Allow-Headers': 'Content-Type',
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "3600",
        }
        return ("", 204, headers)

    body = request.get_json()
    if body is None:
        msg = "Received an empty body request."
        logging.error(msg)
        return msg, 400
    if "cleaned_query" not in body:
        msg = "The cleaned_query field was not provided."
        logging.error(msg)
        return msg, 400
    if "query" not in body:
        msg = "The query field was not provided."
        logging.error(msg)
        return msg, 400

    batch_id = body.get("batch_id")
    if batch_id is not None and not isinstance(batch_id, str):
        return "batch_id should be a string"

    triggered_by = {
        "source": "client" if batch_id is None else "batch",
        "batch_id": batch_id,
    }

    cleaned_query = body["cleaned_query"]
    product_search = {
        "name": body["query"],
        "path_name": body["cleaned_query"],
        "created_at": int(time.time() * 1000),  # ms since epoch
        "search_completed": False,
        "triggered_by": triggered_by,
    }
    try:
        db.reference("product_search").child(cleaned_query).set(product_search)
    except TypeError as ex:
        logging.error(ex)
        return "The request body is not serializable.", 400
    except db.exceptions.FirebaseError as ex:
        logging.error(ex)
        return "Error when communicating with Firebase server.", 500
    except Exception as ex:
        error_message = "Unexpected error: " + str(ex)
        logging.error(error_message)
        return error_message, 400

    # Set CORS headers for the main request
    response_headers = {"Access-Control-Allow-Origin": "*"}
    return json.dumps({"success": True}), 200, response_headers


def store_finished_offers(event, context):
    payload = json.loads(base64.b64decode(event["data"]))
    product_token = payload.get("product_token")
    user_country = payload.get("user_country")
    gtin = payload.get("gtin")
    logging.info(f"Storing offers for product with product_token: {product_token}")

    offer_search = db.reference("offers/SE").child(product_token).get()
    if not offer_search.get("offer_fetch_complete"):
        raise Exception("Trying to store an offer search that is incomplete")

    fetched_offers = offer_search.get("fetched_offers", [])
    product_id = offer_search.get("product_id")

    store_offers_in_bq(product_id, product_token, fetched_offers)

    best_offer = get_best_offer(fetched_offers)

    if best_offer is None:
        return None

    store_best_offer_in_db(product_id, product_token, gtin, user_country, best_offer)
