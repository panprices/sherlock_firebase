from src.database.database import connect_to_db

def fetch_gtin_url(gtin):
	cur, cur_dict, connection, pg_pool = connect_to_db()

	cur.execute(cur.mogrify("SELECT offer_source, url FROM offer_urls WHERE gtin = %s", (gtin, )))
	rows = cur.fetchall()

	pg_pool.putconn(connection)
	return rows