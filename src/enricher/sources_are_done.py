from firebase_admin import db


def get_offer_sources():
    return ["ebay", "kelkoo", "pricerunner", "prisjakt"]


def mark_source_as_done(user_country, product_token, offer_source):
    search_ref = db.reference("offers").child(user_country).child(product_token)

    # Mark offer source as done
    offer_source = offer_source
    offer_sources_ref = search_ref.child("offer_sources_done")
    offer_sources_ref.child(offer_source).set(True)

    # Check if all offer sources are done
    offer_sources_done = offer_sources_ref.get() or {}

    all_sources_done = True
    for source in get_offer_sources():
        all_sources_done = all_sources_done and offer_sources_done.get(source, False)

    return all_sources_done
