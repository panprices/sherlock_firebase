from datetime import datetime
from src.database.database import connect_to_db

def get_current_minute():
    current_time = datetime.now()
    return current_time.minute + current_time.hour * 60

def get_popular_product(cutoff_time = get_current_minute(), time_range = 10):
    print(f"Fetching popular product from {cutoff_time - time_range} to {cutoff_time} mins of day")

    cur, cur_dict, connection, pg_pool = connect_to_db()

    cur.execute("""
        SELECT
            product_token
        FROM products
        WHERE popularity_idx IS NOT NULL AND rand_min_in_day >= {0} AND rand_min_in_day < {1}
    """.format(cutoff_time - time_range, cutoff_time))
    rows = cur.fetchall()

    pg_pool.putconn(connection)

    # the returned row is in the form [(token, ), (token, ),...]
    # turn it into [token, token,...]
    return [row[0] for row in rows]