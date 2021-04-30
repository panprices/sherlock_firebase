
		WITH offers_data AS (
			
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'34ab234e-8dc4-4546-8e62-7ddea9ab47fc' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Steiner 7x50 Navigator Pro Compass' AS retail_prod_name,
				'CyberPhoto' AS retailer_name,
				'SE' AS country,
				'606900' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwjj57O37JDtAhUK97MKHWqEDkIYABAHGgJxbg&sig=AOD64_2BS02e6034qaP3NB-6maB48b5RJQ&ctype=5&q=&ved=0ahUKEwiDwLG37JDtAhXrFFkFHWVUA_8Q2ikIEw&adurl=' AS offer_url,
				'2020-11-20T09:50:40Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'c6cb8138-a5a8-4a10-b9aa-b6c1728edacc' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Steiner 7x50 Navigator Pro Compass' AS retail_prod_name,
				'Gunnar Olssons Foto' AS retailer_name,
				'SE' AS country,
				'649000' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwjj57O37JDtAhUK97MKHWqEDkIYABAEGgJxbg&sig=AOD64_0_-ymlxEJ1AQZgms6aUuc7uaD9pw&ctype=5&q=&ved=0ahUKEwiDwLG37JDtAhXrFFkFHWVUA_8Q2ikIFg&adurl=' AS offer_url,
				'2020-11-20T09:50:40Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'e9d45175-03cb-4716-89ea-70813c446b48' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Steiner 7x50 Navigator Pro Compass' AS retail_prod_name,
				'Natur.Astrosweden.se' AS retailer_name,
				'SE' AS country,
				'681900' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwjj57O37JDtAhUK97MKHWqEDkIYABAGGgJxbg&sig=AOD64_3BhgH549taGEI9sxgzPtvp-4yQgA&ctype=5&q=&ved=0ahUKEwiDwLG37JDtAhXrFFkFHWVUA_8Q2ikIGQ&adurl=' AS offer_url,
				'2020-11-20T09:50:40Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'391a8366-55a1-479d-b903-29f8e6fbe529' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Steiner 7x50 Navigator Pro Compass' AS retail_prod_name,
				'Raig Se' AS retailer_name,
				'SE' AS country,
				'554528' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwjj57O37JDtAhUK97MKHWqEDkIYABAFGgJxbg&sig=AOD64_0o7Plv3Vxv5QymGtEgDC6fftJiuA&ctype=5&q=&ved=0ahUKEwiDwLG37JDtAhXrFFkFHWVUA_8Q2ikIHA&adurl=' AS offer_url,
				'2020-11-20T09:50:40Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'4e170359-a69e-44aa-8004-6b3c0788f903' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Steiner 7x50 Navigator Pro Compass' AS retail_prod_name,
				'Knivesandtools.com' AS retailer_name,
				'SE' AS country,
				'601367' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwjj57O37JDtAhUK97MKHWqEDkIYABAKGgJxbg&sig=AOD64_1WoiaAuobOTU8PbxphX_-ftcIdMg&ctype=5&q=&ved=0ahUKEwiDwLG37JDtAhXrFFkFHWVUA_8Q2ikIHw&adurl=' AS offer_url,
				'2020-11-20T09:50:40Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'79de961c-b5fa-4017-bf89-3c7442ee41bf' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Steiner 7x50 Navigator Pro Compass' AS retail_prod_name,
				'Watski.se' AS retailer_name,
				'SE' AS country,
				'649500' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwjj57O37JDtAhUK97MKHWqEDkIYABALGgJxbg&sig=AOD64_2388xRx43pH7GaiG6WtcKE2Krggw&ctype=5&q=&ved=0ahUKEwiDwLG37JDtAhXrFFkFHWVUA_8Q2ikIIw&adurl=' AS offer_url,
				'2020-11-20T09:50:40Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				NULL AS offer_id,
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
				DISTINCT ON (A.offer_id)
				A.*,
				B.ship,
		--		((B.min_order_val * C.to_sek) / 100)::int AS min_order_val, -- adjusted for the currency
		--		((B.fee * C.to_sek) / 100)::int AS fee, -- adjusted for the currency
				CASE
					-- When we don't have shipping data => return estimation in some cases
					WHEN fee IS NULL THEN
						CASE
							WHEN A.country IN ('UK', 'IT', 'ES') THEN 406
							WHEN A.country IN ('FR', 'BE', 'LU', 'DE') THEN 203
							WHEN A.country = 'SE' THEN 0
							ELSE NULL
						END
					-- When we have shipping and item price is higher then min order value => return the fee
					WHEN (((B.min_order_val * C.to_sek) / 100) > A.adj_price) THEN ((B.fee * C.to_sek) / 100)::int
					-- When we have shipping and there is no min order value => return the fee
					WHEN (min_order_val IS NULL AND fee IS NOT NULL) THEN ((B.fee * C.to_sek) / 100)::int
					ELSE NULL
				END AS shipping_fee
			FROM offers_raw A
			FULL OUTER JOIN shipping B
			ON A.retailer_id = B.retailer_id
			FULL OUTER JOIN currency C
			ON B.currency = C.name
		-- Add retailer trust data and determine quality score
		), offers_with_shipping_and_trust AS (
			SELECT
				DISTINCT ON (A.offer_id)
				A.*,
				(adj_price + shipping_fee) * 0.03 AS exchange_rate_fee,
				(adj_price + shipping_fee) * 0.05 AS service_fee,
				((adj_price + shipping_fee) * 0.05) * 0.25 AS vat,
				(adj_price + shipping_fee + ((adj_price + shipping_fee) * 0.05) + (((adj_price + shipping_fee) * 0.05) * 0.25)) * 0.014 AS payment_fee_se,
				(adj_price + shipping_fee + ((adj_price + shipping_fee) * 0.03) + ((adj_price + shipping_fee) * 0.05) + (((adj_price + shipping_fee) * 0.05) * 0.25)) * 0.014 AS payment_fee_int,
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
			ON A.offer_id = C.offer_id
			WHERE C.offer_id IS NULL
			-- Blacklisted retailers
			AND lower(A.retailer_name) NOT SIMILAR TO 'bluecity%|datapryl%'
		), offers_complete AS (
			SELECT
				*,
				--((SELECT * FROM lowest_local_price) - B.adj_price) AS saving,
				CASE
					WHEN shipping_fee IS NOT NULL AND offer_source != 'google_shopping_SE' AND country != 'SE' THEN true
					ELSE false
				END AS direct_checkout,
				CASE
					WHEN shipping_fee IS NOT NULL AND offer_source != 'google_shopping_SE' AND country != 'SE' THEN
						CASE
							WHEN country != 'SE' THEN ((adj_price + shipping_fee + service_fee + vat + payment_fee_int + exchange_rate_fee) )
							ELSE NULL
						END
					ELSE NULL
				END AS direct_checkout_price,
				CASE
					WHEN quality_score > 5 THEN 5
					WHEN quality_score < 1 THEN 1
					ELSE quality_score::float
				END AS quality_score_adjusted
			FROM (
				SELECT
					*
				FROM offers_filtered
			) B
		)
		SELECT
			updated_at,
			product_id,
			offer_source,
			retail_prod_name,
			retailer_name,
			country,
			(adj_price * 100)::int AS price,
			'SEK' AS currency,
			offer_url,
			quality_score_adjusted AS quality_score,
			domain,
			CASE -- Ship should be true when direct_checkout is enabled
				WHEN direct_checkout IS TRUE THEN TRUE
				ELSE ship
			END AS ship,
			(shipping_fee * 100)::int AS shipping_fee,
			(((SELECT * FROM lowest_local_price) - adj_price) * 100)::int AS saving,
			CASE -- Only show saving when direct_checkout is enabled
				WHEN direct_checkout IS TRUE THEN
					CASE -- Only show saving when the offer is less expensive then the Swedish one
				 		WHEN ((SELECT * FROM lowest_local_price) > direct_checkout_price) THEN (((SELECT * FROM lowest_local_price) - direct_checkout_price) * 100)::int
						ELSE NULL
					END
				ELSE NULL
			END AS direct_checkout_saving,
			offer_id,
			direct_checkout,
			(direct_checkout_price * 100)::int AS direct_checkout_price,
			CASE -- Don't enable concierge on Swedish offers or when direct_checkout is enabled
				WHEN direct_checkout IS FALSE AND country != 'SE' THEN TRUE
				ELSE FALSE
			END AS concierge
		FROM offers_complete
		WHERE offer_source IS NOT NULL-- Remove the row needed for the union
		AND offer_source NOT LIKE 'google_shopping%'-- TEMPORARY REMOVE GOOGLE SHOPPING
		ORDER BY direct_checkout_price ASC;
	