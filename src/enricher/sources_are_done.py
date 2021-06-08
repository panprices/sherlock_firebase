from firebase_admin import db


def mark_source_as_done(user_country, product_token, offer_source):
    search_ref = db.reference("offers").child(user_country).child(product_token)

    # Mark offer source as done
    offer_source = offer_source
    offer_sources_ref = search_ref.child("offer_sources_done")
    offer_sources_ref.child(offer_source).set(True)

    # Check if all offer sources are done
    offer_sources_done = offer_sources_ref.get() or {}
    for source, done in offer_sources_done:
        if not done:
            return False

    return True
