
		WITH offers_data AS (
			
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'5c2d829d-44ba-45fc-bcbd-d3b36ba6aae4' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Nikon D850 Kamerahus' AS retail_prod_name,
				'CyberPhoto' AS retailer_name,
				'SE' AS country,
				'2892900' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwiT17z38JDtAhUB5bMKHeNFCG4YABAGGgJxbg&sig=AOD64_0QofdHdD45hOQm0JtYxzRnIYp0eg&ctype=5&q=&ved=0ahUKEwj01rj38JDtAhXcEVkFHdjQCVAQ2ikIEw&adurl=' AS offer_url,
				'2020-11-20T10:10:48Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'90a75df9-b7fd-4c31-93dc-96a4f31db760' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Nikon D850 Kamerahus' AS retail_prod_name,
				'Scandinavian Photo SE' AS retailer_name,
				'SE' AS country,
				'2885000' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwiT17z38JDtAhUB5bMKHeNFCG4YABAFGgJxbg&sig=AOD64_3fTzqqtY4s5NrmAH-PEatMooWYPg&ctype=5&q=&ved=0ahUKEwj01rj38JDtAhXcEVkFHdjQCVAQ2ikIFg&adurl=' AS offer_url,
				'2020-11-20T10:10:48Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'455ebeb2-0019-473a-a2ec-d25385c80581' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Nikon D850 Kamerahus' AS retail_prod_name,
				'E-Infinity Online Camera Store' AS retailer_name,
				'SE' AS country,
				'2234710' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwiT17z38JDtAhUB5bMKHeNFCG4YABAEGgJxbg&sig=AOD64_1_2jTkrT6twPZcJJlrQz4q_2USlQ&ctype=5&q=&ved=0ahUKEwj01rj38JDtAhXcEVkFHdjQCVAQ2ikIGQ&adurl=' AS offer_url,
				'2020-11-20T10:10:48Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'91a1e370-ab8d-4447-9747-30932b20ea87' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Nikon D850 Kamerahus' AS retail_prod_name,
				'CameraWorld' AS retailer_name,
				'SE' AS country,
				'2650178' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwiT17z38JDtAhUB5bMKHeNFCG4YABAHGgJxbg&sig=AOD64_1G6W0L5RQjBtYZyp74-rYxmIKzeA&ctype=5&q=&ved=0ahUKEwj01rj38JDtAhXcEVkFHdjQCVAQ2ikIHQ&adurl=' AS offer_url,
				'2020-11-20T10:10:48Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'493aea25-7da7-43a8-ae43-7f2081760e4e' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Nikon D850 Kamerahus' AS retail_prod_name,
				'RajalaProShop' AS retailer_name,
				'SE' AS country,
				'2914000' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwiT17z38JDtAhUB5bMKHeNFCG4YABANGgJxbg&sig=AOD64_2cMwiLvfD7nEic4CzWSIg9rd5hYw&ctype=5&q=&ved=0ahUKEwj01rj38JDtAhXcEVkFHdjQCVAQ2ikIIQ&adurl=' AS offer_url,
				'2020-11-20T10:10:48Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'd41f041a-200a-4d8e-bcd8-8a0bfcc4a88d' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Nikon D850 Kamerahus' AS retail_prod_name,
				'Digitaland' AS retailer_name,
				'SE' AS country,
				'2179682' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwiT17z38JDtAhUB5bMKHeNFCG4YABAMGgJxbg&sig=AOD64_3rwUdWnIlUbQGwwcmnhHGNguxldw&ctype=5&q=&ved=0ahUKEwj01rj38JDtAhXcEVkFHdjQCVAQ2ikIJA&adurl=' AS offer_url,
				'2020-11-20T10:10:48Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'e7c4b517-a45e-4f8c-99e9-5e5281528c9e' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Nikon D850 Kamerahus' AS retail_prod_name,
				'Hegethorns Foto' AS retailer_name,
				'SE' AS country,
				'3084900' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwiT17z38JDtAhUB5bMKHeNFCG4YABAOGgJxbg&sig=AOD64_2ahdMjxY6yl7gqKEvFfZhDDYNvrg&ctype=5&q=&ved=0ahUKEwj01rj38JDtAhXcEVkFHdjQCVAQ2ikIJw&adurl=' AS offer_url,
				'2020-11-20T10:10:48Z' AS requested_at,
				NULL::int AS match_score
			UNION ALL
		
			SELECT
				-- We need to create a random ID since the existing
				-- joins expects that
				'cba97752-381d-4580-81f0-45bbf6240d15' AS offer_id,
				NULL AS updated_at,
				NULL AS product_id,
				'google_shopping_SE' AS offer_source,
				'Nikon D850 Kamerahus' AS retail_prod_name,
				'Photospecialist.com' AS retailer_name,
				'SE' AS country,
				'2959551' AS price,
				'SEK' AS currency,
				'https://www.google.com/aclk?sa=L&ai=DChcSEwiT17z38JDtAhUB5bMKHeNFCG4YABAPGgJxbg&sig=AOD64_0D7BTfcflzTgx1-z_J78DXOc9lDw&ctype=5&q=&ved=0ahUKEwj01rj38JDtAhXcEVkFHdjQCVAQ2ikIKg&adurl=' AS offer_url,
				'2020-11-20T10:10:48Z' AS requested_at,
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
	