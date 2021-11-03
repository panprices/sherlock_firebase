import uuid

from src.database.database import connect_to_db
from pipetools import pipe, maybe, X
from workalendar.europe import Sweden as SwedishCalendar
from datetime import date


_calendar = SwedishCalendar()


"""
    This is necessary since Firebase does not sture keys with
    a value set to null (which seems really stupid).
    Grab the same structure as offer_consumer expects:
    https://github.com/panprices/sherlock_db_functions/blob/master/main.py#L68
"""


def offer_to_tup(offer):
    return (
        offer.get("offer_id") or str(uuid.uuid4()),
        offer.get("product_id") or None,
        offer.get("offer_source") or None,
        offer.get("retail_prod_name") or None,
        offer.get("retailer_name") or None,
        offer.get("country") or None,
        offer.get("price") or None,
        offer.get("currency") or None,
        offer.get("offer_url") or None,
        offer.get("requested_at") or None,
        offer.get("match_score") or None,
        offer.get("stock_status") or None,
    )


"""
    ##################### HOW THIS WORKS ######################
    We are here grabbing the batch of offers and with some PSQL
    magic are able to send that the data to the database server
    which upon receiving will load it into memory and use it to
    do all the joins and stuff that we need to enrich this data.

    The query might look a little messy but we are using the same
    one as we do in the API with the exception that we have to plug
    in our offers data here and then get all the associated data.
"""


def add_offers_metadata(offers, user_country="SE"):
    if user_country != "SE" and user_country != "FI":
        raise ValueError("user_country has to be 'SE' or 'FI'")

    currency_translation = "to_sek" if user_country == "SE" else "to_eur"

    # Open a connection to the database
    cur, cur_dict, connection, pg_pool = connect_to_db()
    # Concatinate the input data to a long string
    offers_str = b"".join(
        cur_dict.mogrify(
            b"""
            SELECT
                -- We need to create a random ID since the existing
                -- joins expects that
                %s AS offer_id,
                NULL AS updated_at,
                %s AS product_id,
                %s AS offer_source,
                %s AS retail_prod_name,
                %s AS retailer_name,
                %s AS country,
                %s AS price,
                %s AS currency,
                %s AS offer_url,
                %s AS requested_at,
                %s::int AS match_score,
                %s AS stock_status
            UNION ALL
        """,
            offer_to_tup(offer),
        )
        for offer in offers
        if offer.get("price") is not None
    )
    cur_dict.execute(
        b"""
        WITH offers_data AS (
            """
        + offers_str
        + f"""
            SELECT
                NULL AS offer_id,
                NULL AS updated_at,
                NULL AS product_id,
                NULL AS offer_source,
                NULL AS retailer_product_name,
                NULL AS retailer_name,
                NULL AS country,
                NULL AS price,
                NULL AS currency,
                NULL AS offer_url,
                NULL AS requested_at,
                NULL AS match_score,
                NULL AS stock_status
        ), offers_raw AS ( ---- take retailer data, calculate the price and filter out blacklisted retailer
            SELECT
                A.*,
                C.domain,
                C.id AS retailer_id,
                C.offer_source_id,
                ((A.price::int * E.{currency_translation}) / 100)::int AS adj_price, -- the price adjusted for the currency
                (A.price::int * E.to_eur)::int AS euro_price, -- TODO: REMOVE
                F.ship,
                F.fee as shipping_fee,
                F.min_order_val as shipping_min_order_val,
                COALESCE(F.min_delivery_time, 5) AS min_delivery_time,
                COALESCE(F.max_delivery_time, 15) AS max_delivery_time,
                G.{currency_translation} as shipping_to_local_currency
            FROM offers_data A
            INNER JOIN offer_sources B
                ON A.offer_source = B.name
            FULL OUTER JOIN retailers C
                ON A.retailer_name = C.name AND B.id = C.offer_source_id
            INNER JOIN currency E
                ON A.currency = E.name
            FULL OUTER JOIN shipping F
                ON C.id = F.retailer_id
            FULL OUTER JOIN currency G
                ON F.currency = G.name
            WHERE COALESCE(C.blacklisted, FALSE) IS FALSE -- Remove blacklisted retailers
        -- Add shipping data (where we have it) to the raw offers rows
        ), offers_with_shipping_and_trust AS (
            SELECT
                DISTINCT ON (A.offer_id)
                A.*,
                B.site_rank as alexa_site_rank,
                C.num_ratings as trustpilot_num_rating,
                C.avg_rating as trustpilot_avg_rating
            FROM offers_raw A
            FULL OUTER JOIN alexa B
            ON A.domain = B.retailer_domain
            FULL OUTER JOIN trustpilot C
            ON A.domain = C.retailer_domain
        ), lowest_local_price AS ( ---- get the lowest price among offers, this step touch all the offers including old and new
            SELECT
                MIN(adj_price)
            FROM offers_with_shipping_and_trust
            WHERE country = '{user_country}' -- Change this when we launch another country
        /*
            Filter out offers:
                1. That do not pass the string matching test.
                2. Which are from black listed retailers (second hand for example)
                3. Which we have duplicates on, for example the same from prisjakt as pricerunner
        */
        ), offers_filtered AS (
            SELECT
                A.*
            FROM offers_with_shipping_and_trust A
            LEFT JOIN (
                SELECT
                    A.*
                FROM offers_with_shipping_and_trust A
                -- Grab the offer sources to filter dynamically to be able to configure
                -- this on the fly without having to re-deploy stuff
                WHERE A.offer_source SIMILAR TO (
                    SELECT
                        array_to_string(
                                array(
                                        SELECT
                                            name
                                        FROM offer_sources
                                        WHERE filter IS TRUE
                                ), '|'
                        )
                )
                AND (
                    (A.adj_price < (SELECT * FROM lowest_local_price) *  (SELECT value FROM offer_filters WHERE name = 'min_price')) OR
                    (A.adj_price > (SELECT * FROM lowest_local_price) *  (SELECT value FROM offer_filters WHERE name = 'max_price'))
                )
            ) C
            ON A.offer_id = C.offer_id
            WHERE C.offer_id IS NULL
            -- Blacklisted retailers
            AND lower(A.retailer_name) NOT SIMILAR TO 'bluecity%|datapryl%|%rakuten%'
        )
        SELECT
            -- Filter out when we from different sources have gotten the same offer
            DISTINCT ON (retail_prod_name, domain, adj_price)

            (SELECT * FROM lowest_local_price) AS lowest_local_price,
            updated_at,
            product_id,
            offer_source,
            retail_prod_name,
            retailer_name,
            country,
            adj_price,
            offer_url,
            domain,
            ship,
            shipping_fee,
            shipping_min_order_val,
            shipping_to_local_currency,
            min_delivery_time,
            max_delivery_time,
            offer_id,
            euro_price,
            trustpilot_num_rating,
            trustpilot_avg_rating,
            alexa_site_rank,
            stock_status
        FROM offers_filtered
        WHERE offer_source IS NOT NULL-- Remove the row needed for the union
        AND offer_source NOT LIKE 'google_shopping%'-- TEMPORARY REMOVE GOOGLE SHOPPING;
    """.encode()
    )
    rows = cur_dict.fetchall()
    """
        IF NEEDED LOG QUERY TO CONSOLE WITH:
        query_string = cur_dict.query.decode()
        print(query_string)
    """
    pg_pool.putconn(connection)

    enrich_row = (
        pipe
        | (_compose_enriched_row, user_country)
        | _translate_subunits_to_units
        | _strip_columns
    )

    rows = map(enrich_row, rows)

    rows = sorted(
        rows,
        key=lambda x: (x["direct_checkout_price"] is None, x["direct_checkout_price"]),
    )

    return rows


def _translate_subunits_to_units(row):
    translate_to_units = maybe | X * 100 | round

    row["shipping_fee"] = translate_to_units(row["shipping_fee"])
    row["direct_checkout_price"] = translate_to_units(row["direct_checkout_price"])
    row["adj_price"] = translate_to_units(row["adj_price"])

    return row


def _strip_columns(row):
    row["price"] = row["adj_price"]

    del row["lowest_local_price"]
    del row["adj_price"]
    del row["trustpilot_num_rating"]
    del row["trustpilot_avg_rating"]
    del row["alexa_site_rank"]
    del row["shipping_min_order_val"]
    del row["shipping_to_local_currency"]
    return row


def _compose_enriched_row(user_country, row):
    # ==========================================================
    # Calculate quality_score
    #
    # It is clamped between 1 and 5
    # ==========================================================
    row["quality_score"] = (
        _calculate_quality_score(
            row["trustpilot_avg_rating"],
            row["trustpilot_num_rating"],
            row["alexa_site_rank"],
        )
        > maybe | (min, 5) | (max, 1) | float
    )

    # ==========================================================
    # Calculate saving
    # ==========================================================
    row["saving"] = _calculate_saving(row)

    # ==========================================================
    # Calculate Shipping Fee
    # ==========================================================
    row["shipping_fee"] = _calculate_shipping_fee(user_country, row)

    # ==========================================================
    # Calculate Delivery Dates
    # ==========================================================
    row["min_delivery_date_with_margin"] = _calculate_delivery_date(
        row["min_delivery_time"]
    )
    row["max_delivery_date_with_margin"] = _calculate_delivery_date(
        row["max_delivery_time"]
    )

    # ==========================================================
    # Calculate Direct Checkout, DC Price, and DC Saving
    # ==========================================================
    row["direct_checkout"] = _calculate_direct_checkout(user_country, row)
    row["direct_checkout_price"] = _calculate_direct_checkout_price(row)
    row["direct_checkout_saving"] = _calculate_direct_checkout_saving(row)

    # ==========================================================
    # Calculate if we ship
    #
    # Ship should be true when direct_checkout is enabled
    # ==========================================================
    row["ship"] = row["direct_checkout"] or row.get("ship")

    # ==========================================================
    # Set currency
    # ==========================================================
    row["currency"] = "SEK" if user_country == "SE" else "EUR"

    # ==========================================================
    # Set stock_status
    # ==========================================================
    row["stock_status"] = row.get("stock_status") or "unknown"

    return row


def _calculate_delivery_date(delivery_time):
    # Add 1 day margin for ops to place the order
    ops_placement_margin = 1

    return _calendar.add_working_days(
        date.today(),
        delivery_time + ops_placement_margin,
    ).strftime("%Y-%m-%d")


def _calculate_saving(row):
    adj_price = row["adj_price"]
    lowest_local_price = row["lowest_local_price"]

    if lowest_local_price is None or adj_price is None:
        return None
    else:
        return round((lowest_local_price - adj_price) * 100)


def _calculate_direct_checkout(user_country, row):
    shipping_fee = row["shipping_fee"]
    offer_source = row["offer_source"]
    country = row["country"]
    adj_price = row["adj_price"]

    if adj_price is None:
        return False

    # Don't allow checkout on products cheaper than 1500 SEK (or 150 EUR)
    # Note: we assume that adj_price is in SEK here
    if adj_price < 1500:
        return False
    if shipping_fee is None:
        return False
    # If offer source is google_shopping_SE etc.
    if offer_source == f"google_shopping_{user_country}":
        return False
    if country == user_country:
        return False

    return True


def _calculate_direct_checkout_saving(row):
    lowest_local_price = row["lowest_local_price"]
    direct_checkout = row["direct_checkout"]
    direct_checkout_price = row["direct_checkout_price"]

    # Only show saving when direct_checkout is enabled
    if (
        direct_checkout
        and lowest_local_price is not None
        and direct_checkout_price is not None
        and lowest_local_price > direct_checkout_price
    ):
        return round((lowest_local_price - direct_checkout_price) * 100)
    else:
        return None


def _calculate_direct_checkout_price(row):
    direct_checkout = row["direct_checkout"]
    adj_price = row["adj_price"]
    shipping_fee = row["shipping_fee"]

    if adj_price is None or shipping_fee is None or not direct_checkout:
        return None

    exchange_rate_fee = (adj_price + shipping_fee) * 0.03
    service_fee = (adj_price + shipping_fee) * 0.06
    vat = service_fee * 0.25
    payment_fee_int = (
        adj_price + shipping_fee + exchange_rate_fee + service_fee + vat
    ) * 0.014

    return float(
        adj_price
        + shipping_fee
        + service_fee
        + vat
        + payment_fee_int
        + exchange_rate_fee
    )


def _calculate_quality_score(
    trustpilot_avg_rating, trustpilot_num_rating, alexa_site_rank
):

    if trustpilot_avg_rating is None:
        return None

    # No trustpilot rating
    if trustpilot_avg_rating == 0:
        if alexa_site_rank is None:
            return None

        if alexa_site_rank < 15000:
            return 5.0
        elif alexa_site_rank < 25000:
            return 4.5
        elif alexa_site_rank < 50000:
            return 4.0
        elif alexa_site_rank < 75000:
            return 3.0
        elif alexa_site_rank < 100000:
            return 2.0
        elif alexa_site_rank < 300000:
            return 1.5
        elif alexa_site_rank < 500000:
            return 1.0
    # If we have Truspilot rating, start with that rating and then credit
    # or discredit it
    else:
        quality_score = trustpilot_avg_rating

        if trustpilot_num_rating is None:
            quality_score = quality_score
        elif trustpilot_num_rating < 50:
            quality_score = quality_score - 1.5
        elif trustpilot_num_rating < 100:
            quality_score = quality_score - 1
        elif trustpilot_num_rating > 1000:
            quality_score = quality_score + 0.5
        elif trustpilot_num_rating > 5000:
            quality_score = quality_score + 1

        if alexa_site_rank is None:
            quality_score = quality_score - 1
        elif alexa_site_rank < 15000:
            quality_score = quality_score + 1
        elif alexa_site_rank < 25000:
            quality_score = quality_score + 0.5
        elif alexa_site_rank > 100000:
            quality_score = quality_score - 0.5
        elif alexa_site_rank > 300000:
            quality_score = quality_score - 1

        return quality_score


def _calculate_shipping_fee(user_country, row):
    shipping_min_order_val = row["shipping_min_order_val"]
    shipping_to_local_currency = row["shipping_to_local_currency"]
    shipping_fee = row["shipping_fee"]
    adj_price = row["adj_price"]
    country = row["country"]

    # When we don't have shipping data => return estimation in some cases
    if shipping_fee is None:
        if country in {"UK", "IT", "ES"}:
            return 406
        elif country in {"FR", "BE", "LU", "DE"}:
            return 203
        elif country == user_country:
            return 0
        else:
            return None
    # When we have shipping and there is no min order value => return the fee
    elif shipping_min_order_val is None:
        return round((shipping_fee * shipping_to_local_currency) / 100)
    # When we have shipping and item price is higher than min order value => return the fee
    elif ((shipping_min_order_val * shipping_to_local_currency) / 100) > adj_price:
        return round((shipping_fee * shipping_to_local_currency) / 100)
    else:
        return None
