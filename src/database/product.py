from datetime import datetime
from typing import Union
from src.database.database import connect_to_db


def get_popular_products():
    cutoff_time = _get_current_minute()
    return _get_popular_products(cutoff_time)


def _get_current_minute():
    current_time = datetime.now()
    return current_time.minute + current_time.hour * 60


def _get_popular_products(cutoff_time, time_range=10):
    print(
        f"Fetching popular product from {cutoff_time - time_range} to {cutoff_time} mins of day"
    )

    cur, cur_dict, connection, pg_pool = connect_to_db()

    cur.execute(
        """
        -- Grab all the popular products
		SELECT
            product_token,
            id
        FROM products
        WHERE popularity_idx IS NOT NULL
        AND rand_min_in_day >= {0}
        AND rand_min_in_day < {1}
        AND product_token IS NOT NULL
        -- Grab the products which we are doing campaigns on
        UNION ALL
        SELECT
            A.product_token,
            A.id
        FROM products A
            INNER JOIN campaign_products B
                ON A.id::text = B.product_id
        WHERE A.rand_min_in_day >= {0}
        AND A.rand_min_in_day < {1}
        AND product_token IS NOT NULL
        -- Grab the products from store offers
        UNION ALL
        SELECT 
            A.product_token,
            A.id
        FROM products A
            INNER JOIN panprices_offers B
                ON A.id = B.product_id
        WHERE A.rand_min_in_day >= {0}
        AND A.rand_min_in_day < {1}
        AND product_token IS NOT NULL
    """.format(
            cutoff_time - time_range, cutoff_time
        )
    )
    rows = cur.fetchall()

    pg_pool.putconn(connection)

    # the returned row is in the form [(token, id), (token, id),...]
    # turn it into [token, token,...]
    return [(row[0], row[1]) for row in rows]


def get_gtin_from_product_id(product_id: int) -> Union[None, str]:
    cur, cur_dict, connection, pg_pool = connect_to_db()

    cur.execute(
        """
    SELECT gtin
    FROM products
    WHERE id = %(product_id)s
    """,
        {"product_id": product_id},
    )

    row = cur.fetchone()
    if row is None:
        return None
    else:
        return row[0]
