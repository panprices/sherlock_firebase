from src.database.database import connect_to_db


def get_best_offer(offers):
    """Return the direct-checkout-able offer (if any) with the lowest
    direct_checkout_price, else return the offer with the lowest price.
    """
    if len(offers) == 0:
        return None

    direct_checkout_offers = [offer for offer in offers if offer.get("direct_checkout")]
    valid_dc_offers = [
        offer
        for offer in direct_checkout_offers
        if offer.get("direct_checkout_price") is not None
    ]
    valid_offers = [offer for offer in offers if offer.get("price") is not None]

    if len(valid_dc_offers) == 0 and len(valid_offers) == 0:
        return None

    if len(valid_dc_offers) != 0:
        best_offer = min(
            valid_dc_offers, key=lambda offer: offer["direct_checkout_price"]
        )

    else:
        best_offer = min(valid_offers, key=lambda offer: offer["price"])

    return best_offer


def store_best_offer_in_db(product_id, product_token, gtin, user_country, best_offer):
    db_row = {
        "product_id": product_id,
        "product_token": product_token,
        "gtin": gtin,
        "user_country": user_country,
        "currency": best_offer.get("currency"),
        "offer_source": best_offer.get("offer_source"),
        "country": best_offer.get("country"),
        "price": best_offer.get("price"),
        "saving": best_offer.get("saving"),
        "retailer_name": best_offer.get("retailer_name"),
        "domain": best_offer.get("domain"),
        "offer_url": best_offer.get("offer_url"),
        "quality_score": best_offer.get("quality_score"),
        "retail_prod_name": best_offer.get("retail_prod_name"),
        "ship": best_offer.get("ship"),
        "shipping_fee": best_offer.get("shipping_fee"),
        "direct_checkout": best_offer.get("direct_checkout"),
        "direct_checkout_price": best_offer.get("direct_checkout_price"),
        "direct_checkout_saving": best_offer.get("direct_checkout_saving"),
    }

    cur, cur_dict, connection, pg_pool = connect_to_db()

    print("gtin", gtin)

    cur.execute(
        b"""
            INSERT INTO best_offers (
                gtin,
                user_country,
                product_id,
                country,
                currency,
                offer_source,
                price,
                saving,
                retailer_name,
                domain,
                offer_url,
                quality_score,
                retail_prod_name,
                ship,
                shipping_fee,
                direct_checkout,
                direct_checkout_price,
                direct_checkout_saving
            ) VALUES (
                %(gtin)s,
                %(user_country)s,
                %(product_id)s,
                %(country)s,
                %(currency)s,
                %(offer_source)s,
                %(price)s,
                %(saving)s,
                %(retailer_name)s,
                %(domain)s,
                %(offer_url)s,
                %(quality_score)s,
                %(retail_prod_name)s,
                %(ship)s,
                %(shipping_fee)s,
                %(direct_checkout)s,
                %(direct_checkout_price)s,
                %(direct_checkout_saving)s
            )
            ON CONFLICT (gtin) DO UPDATE SET
                user_country = EXCLUDED.user_country,
                country = EXCLUDED.country,
                currency = EXCLUDED.currency,
                offer_source = EXCLUDED.offer_source,
                price = EXCLUDED.price,
                saving = EXCLUDED.saving,
                retailer_name = EXCLUDED.retailer_name,
                domain = EXCLUDED.domain,
                offer_url = EXCLUDED.offer_url,
                quality_score = EXCLUDED.quality_score,
                retail_prod_name = EXCLUDED.retail_prod_name,
                ship = EXCLUDED.ship,
                shipping_fee = EXCLUDED.shipping_fee,
                direct_checkout = EXCLUDED.direct_checkout,
                direct_checkout_price = EXCLUDED.direct_checkout_price,
                direct_checkout_saving = EXCLUDED.direct_checkout_saving
            ;
        """,
        db_row,
    )

    connection.commit()
    pg_pool.putconn(connection)
