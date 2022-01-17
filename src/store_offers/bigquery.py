import uuid
import logging
import datetime

from google.cloud import bigquery


def store_offers_in_bq(product_id, product_token, fetched_offers):
    instance_id = str(uuid.uuid4())
    fetched_at = datetime.datetime.utcnow().isoformat()

    for offer in fetched_offers:
        offer["product_id"] = product_id
        offer["product_token"] = product_token
        offer["instance_id"] = instance_id
        offer["fetched_at"] = fetched_at

    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset("offers").table("offers")
    table = bigquery_client.get_table(table_ref)

    # Remove all properties that have no corresponding table column
    offer_rows = []
    for offer in fetched_offers:
        offer_rows.append(
            {schema.name: offer.get(schema.name) for schema in table.schema}
        )

    if len(offer_rows) == 0:
        return

    errors = bigquery_client.insert_rows(table, offer_rows)
    if errors != []:
        logging.error(str(errors))
        raise Exception(str(errors))
