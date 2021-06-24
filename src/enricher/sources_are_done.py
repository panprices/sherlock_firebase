from firebase_admin import db


def mark_source_as_done(user_country, product_token, offer_source):
    search_ref = db.reference("offers").child(user_country).child(product_token)

    # Mark offer source as done
    offer_source = offer_source
    offer_sources_done_ref = search_ref.child("offer_sources_done")
    offer_sources_done_ref.child(offer_source).set(True)

    # Check if all offer sources are done
    offer_sources_done = offer_sources_done_ref.get().values()
    if False in offer_sources_done:
        return False
    return True
