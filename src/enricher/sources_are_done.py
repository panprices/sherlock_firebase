from firebase_admin import db
from firebase_admin import firestore


def mark_source_as_done(user_country, product_token, offer_source):
    search_ref = db.reference("offers").child(user_country).child(product_token)

    # Mark offer source as done
    offer_source = offer_source
    offer_sources_done_ref = search_ref.child("offer_sources_done")
    offer_sources_done_ref.child(offer_source).set(True)

    # Check if all offer sources are done
    offer_sources_done = offer_sources_done_ref.get()
    if offer_sources_done is None:
        return False
    else:
        return all(offer_sources_done.values())


def mark_source_as_done_fs(user_country, product_id, offer_source):
    f_db = firestore.client()
    search_ref = f_db.collection("offer_search").document(
        f"{product_id}_{user_country}"
    )

    # Mark offer source as done
    # Use set as opposed to update, because when we start demo searches there is no document already in the database
    search_ref.set({f"offer_sources_done.{offer_source}": True})

    # Check if all offer sources are done
    offer_sources_done = search_ref.get().to_dict()["offer_sources_done"]
    print("offer_sources_done", offer_sources_done)
    if offer_sources_done is None:
        return False
    else:
        return all(offer_sources_done.values())
