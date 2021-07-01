from datetime import datetime, timedelta

from firebase_admin import db


def delete_data(bucket, hours_cutoff, firebase_db):
    # Get the timestamp of data and time 1 hour ago
    cutoff = datetime.now() - timedelta(hours=hours_cutoff)
    # The above returns something like 1598616388.782356 get rounded 12 digits
    cutoff_ts = int(float(cutoff.timestamp()) * 1000)
    # Get all the past searches which are older than 1 hour
    items_to_remove = (
        firebase_db.reference(bucket)
        .order_by_child("created_at")
        .end_at(cutoff_ts)
        .get()
    ).keys()
    if len(items_to_remove) == 0:
        print(f"No items to delete in the {bucket} bucket ")
        return

    print(f"Deleting {len(items_to_remove)} items in {bucket} bucket")
    # Iterate over all the paths and delete them one by one.
    items_to_update = {}
    for node in items_to_remove:
        items_to_update[node] = {}

    if len(items_to_update) == 0:
        return

    # Delete them in bulk
    try:
        db.reference(bucket).update(items_to_update)
    except Exception as e:
        raise e

    return
