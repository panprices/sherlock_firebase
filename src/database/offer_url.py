from src.database.database import connect_to_db
import datetime


def fetch_gtin_url(gtin: str) -> dict:
    """Retrieve cached offer_urls.

    Note that if the cached url is NULL and the data is too old, then we would
    try to scrape the offer url again by not including it in the returned data.

    Returns:
        A dictionary that maps (offer_source -> offer_url).
    """
    cur, cur_dict, connection, pg_pool = connect_to_db()

    query = cur_dict.mogrify(
        "SELECT offer_source, url, created_at, updated_at FROM offer_urls WHERE gtin = %s",
        (gtin,),
    )
    cur_dict.execute(query)
    rows = cur_dict.fetchall()
    pg_pool.putconn(connection)

    offer_urls = {}
    for row in rows:
        if not row["url"] and _data_too_old(row["created_at"], row["updated_at"]):
            continue

        offer_urls[row["offer_source"]] = row["url"]

    return offer_urls


def fetch_google_shopping_url(gtin):
    cur, cur_dict, connection, pg_pool = connect_to_db()

    cur.execute(cur.mogrify("SELECT url FROM products WHERE gtin = %s", (gtin,)))
    row = cur.fetchone()

    pg_pool.putconn(connection)
    return row[0] if row else None


def _data_too_old(
    created_at: datetime.datetime, updated_at: datetime.datetime = None
) -> bool:
    MAX_CACHE_DAYS = 7
    if updated_at is not None:
        timestamp = updated_at
    else:
        timestamp = created_at

    now = datetime.datetime.now()
    time_elapsed = now - timestamp
    return True if time_elapsed.days > MAX_CACHE_DAYS else False
