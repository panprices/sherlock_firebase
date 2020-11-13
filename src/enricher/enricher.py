import json
from src.database.database import connect_to_db

'''
	This is necessary since Firebase does not sture keys with
	a value set to null (which seems really stupid).
	Grab the same structure as offer_consumer expects:
	https://github.com/panprices/sherlock_db_functions/blob/master/main.py#L68
'''

def offer_to_tup(offer) :
	return (
		offer.get('product_id') or None,
		offer.get('offer_source') or None,
		offer.get('retail_prod_name') or None,
		offer.get('retailer_name') or None,
		offer.get('country') or None,
		offer.get('price') or None,
		offer.get('currency') or None,
		offer.get('offer_url') or None,
		offer.get('requested_at') or None,
		offer.get('match_score') or None
	)

'''
	##################### HOW THIS WORKS ######################
	We are here grabbing the batch of offers and with some PSQL
	magic are able to send that the data to the database server
	which upon receiving will load it into memory and use it to
	do all the joins and stuff that we need to enrich this data.

	The query might look a little messy but we are using the same
	one as we do in the API with the exception that we have to plug
	in our offers data here and then get all the associated data.
'''

def add_offers_metadata(offers) :
	# Open a connection to the database
	cur, cur_dict, connection, pg_pool = connect_to_db()
	# Concatinate the input data to a long string
	offers_str = b''.join(
		cur_dict.mogrify(b"""
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				floor(random()* (10000 - 1 + 1) + 1) AS offers_raw_id,
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
				%s::int AS match_score
			UNION ALL
		""", offer_to_tup(offer),
		) for offer in offers
	)
	cur_dict.execute(b"""
		WITH offers_data AS (
			""" + offers_str + b"""
			SELECT
				NULL AS offers_raw_id,
				NULL AS updated_at,
				NULL AS product_id,
				NULL AS source,
				NULL AS retailer_product_name,
				NULL AS retailer_name,
				NULL AS country,
				NULL AS price,
				NULL AS currency,
				NULL AS offer_url,
				NULL AS requested_at,
				NULL AS match_score
		), offers_raw AS ( ---- take retailer data, calculate the price and filter out blacklisted retailer
			SELECT
				A.*,
				C.domain,
				C.id AS retailer_id,
				C.offer_source_id,
				((A.price::int * E.to_sek) / 100)::int AS adj_price -- the price adjusted for the currency
			FROM offers_data A
			INNER JOIN offer_sources B
			ON A.offer_source = B.name
			FULL OUTER JOIN retailers C
			ON A.retailer_name = C.name AND B.id = C.offer_source_id
			INNER JOIN currency E
			ON A.currency = E.name
			WHERE COALESCE(C.blacklisted, FALSE) IS FALSE -- Remove blacklisted retailers
		-- Add shipping data (where we have it) to the raw offers rows
		), offers_with_shipping AS (
			SELECT
				DISTINCT ON (A.offers_raw_id)
				A.*,
				B.ship,
		--		((B.min_order_val * C.to_sek) / 100)::int AS min_order_val, -- adjusted for the currency
		--		((B.fee * C.to_sek) / 100)::int AS fee, -- adjusted for the currency
				CASE
					WHEN fee IS NULL THEN NULL
					WHEN (((B.min_order_val * C.to_sek) / 100) > A.adj_price) THEN ((B.fee * C.to_sek) / 100)::int
					WHEN (min_order_val IS NULL AND fee IS NOT NULL) THEN ((B.fee * C.to_sek) / 100)::int
					ELSE 0
				END AS shipping_fee
			FROM offers_raw A
			FULL OUTER JOIN shipping B
			ON A.retailer_id = B.retailer_id
			FULL OUTER JOIN currency C
			ON B.currency = C.name
		-- Add retailer trust data and determine quality score
		), offers_with_shipping_and_trust AS (
			SELECT
				DISTINCT ON (A.offers_raw_id)
				A.*,
				CASE
					-- If no Trustpilot rating
					WHEN avg_rating = 0 THEN
						CASE
							WHEN site_rank < 15000 	THEN 5
							WHEN site_rank < 25000 	THEN 4.5
							WHEN site_rank < 50000 	THEN 4
							WHEN site_rank < 75000 	THEN 3
							WHEN site_rank < 100000 THEN 2
							WHEN site_rank < 300000 THEN 1.5
							WHEN site_rank < 500000 THEN 1
						END
					-- If we have Truspilot rating, start with that rating and then credit or discredit it
					WHEN avg_rating != 0 THEN (
						SELECT (
							avg_rating +
							CASE
								WHEN num_ratings < 50 THEN -1
								WHEN num_ratings < 100 THEN -0.5
								WHEN num_ratings > 1000 THEN 0.5
								WHEN num_ratings > 5000 THEN 1
								ELSE 0
							END +
							CASE WHEN num_ratings < 100 THEN -0.5 ELSE 0 END +
							CASE
								WHEN site_rank < 15000 THEN 1
								WHEN site_rank < 25000 THEN 0.5
								WHEN site_rank > 100000 THEN -0.5
								WHEN (site_rank > 300000) OR (site_rank IS NULL) THEN -1
								ELSE 0
							END
						)
					)
				END AS quality_score
			FROM offers_with_shipping A
			FULL OUTER JOIN alexa B
			ON A.domain = B.retailer_domain
			FULL OUTER JOIN trustpilot C
			ON A.domain = C.retailer_domain
		), lowest_local_price AS ( ---- get the lowest price among offers, this step touch all the offers including old and new
			SELECT
				MIN(adj_price)
			FROM offers_with_shipping_and_trust
			WHERE country = 'SE' -- Change this when we launch another country
		/*
			Filter out offers:
				1. That do not pass the string matching test.
				2. Which are from black listed retailers (second hand for example)
				3. Which we have duplicates on, for example the same from prisjakt as pricerunner
		*/
		), offers_filtered AS (
			SELECT
				-- Filter out when we from different sources have gotten the same offer
				DISTINCT ON (A.retail_prod_name, A.domain, A.adj_price)
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
			ON A.offers_raw_id = C.offers_raw_id
			WHERE C.offers_raw_id IS NULL
			-- Blacklisted retailers
			AND lower(A.retailer_name) NOT SIMILAR TO 'bluecity%|datapryl%'
		), offers_complete AS (
			SELECT
				*,
				((SELECT * FROM lowest_local_price) - B.adj_price) AS saving
			FROM (
				SELECT
					*
				FROM offers_filtered
			) B
			ORDER BY adj_price
		)
		SELECT
			updated_at,
			product_id,
			offer_source,
			retail_prod_name,
			retailer_name,
			country,
			/*
				API does not return price like this to the client but since
				we will fetch from Firebase when the 2nd offer source comes
				in and then have incoming data with two precision points
				and compare to one without it will get wrong.
			*/
			adj_price * 100 AS price,
			'SEK' AS currency,
			offer_url,
			-- TODO: Move this to some other place higher up
			CASE
				WHEN quality_score > 5 THEN 5
				WHEN quality_score < 1 THEN 1
				ELSE quality_score::float
			END AS quality_score,
			domain,
			ship,
			shipping_fee,
			saving * 100 AS saving -- see comment above on prices
		FROM offers_complete
		WHERE offer_source IS NOT NULL; -- Remove the row needed for the union
	""")
	rows = cur_dict.fetchall()
	'''
		IF NEEDED LOG QUERY TO CONSOLE WITH:
		query_string = cur_dict.query.decode()
		print(query_string)
	'''
	pg_pool.putconn(connection)
	return rows
