from typing import Optional
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
        # Reuse NULL cached url only if it is up to date:
        if not row["url"] and not _up_to_date(
            row["offer_source"],
            row["created_at"],
            row["updated_at"],
        ):
            print(
                f"Offer link from {row['offer_source']} for product {gtin} is NULL and is not up to date, does not reuse."
            )
            continue

        offer_urls[row["offer_source"]] = row["url"]

    return offer_urls


def fetch_google_shopping_url(gtin):
    cur, cur_dict, connection, pg_pool = connect_to_db()

    cur.execute(cur.mogrify("SELECT url FROM products WHERE gtin = %s", (gtin,)))
    row = cur.fetchone()

    pg_pool.putconn(connection)
    return row[0] if row else None


def _up_to_date(
    offer_source: str,
    created_at: datetime.datetime,
    updated_at: datetime.datetime = None,
) -> bool:
    """An offer_url is up to date if:
    - it's been created within 30 days (for new products) & updated within 12 hours, or
    - it's been created more than 30 days & updated within 7 days
    """
    OLD_PRODUCT_THRESHHOLD_DAYS = 30
    NEW_PRODUCT_MAX_CACHE_HOURS = 12
    OLD_PRODUCT_MAX_CACHE_DAYS = 7
    IDEALO_NEW_PRODUCT_MAX_CACHE_HOURS = 72  # special rule for Idealo

    # Always reuse cached url from Idealo to reduce cost:
    now = datetime.datetime.now()
    if updated_at is None:
        updated_at = created_at

    since_creation = now - created_at
    since_last_update = now - updated_at
    url_is_new = since_creation.days < OLD_PRODUCT_THRESHHOLD_DAYS

    if url_is_new:
        if "idealo" in offer_source:
            up_to_date = (
                True
                if since_last_update.total_seconds()
                < IDEALO_NEW_PRODUCT_MAX_CACHE_HOURS * 3600
                else False
            )
        else:
            up_to_date = (
                True
                if since_last_update.total_seconds()
                < NEW_PRODUCT_MAX_CACHE_HOURS * 3600
                else False
            )

    if not url_is_new:
        if "idealo" in offer_source:
            up_to_date = True
        else:
            up_to_date = (
                True if since_last_update.days < OLD_PRODUCT_MAX_CACHE_DAYS else False
            )

    return up_to_date
